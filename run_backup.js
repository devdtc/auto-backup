
const archiver = require('archiver');
const assert = require('assert').strict;
const { spawn } = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const glob = require('glob');
const moment = require('moment');
const path = require('path');
const { PassThrough, Transform, Writable, pipeline } = require('stream');
const tar = require('tar-stream');
const { promisify } = require('util');
const zlib = require('zlib');


class BufferStream extends Writable {
  constructor(options) {
    super(options);

    this.chunks = [];
    this.length = 0;
  }

  _write(chunk, encoding, cb) {
    if (!Buffer.isBuffer(chunk)) {
      chunk = new Buffer(chunk);
    }

    this.chunks.push(chunk);
    this.length += chunk.length;

    cb();
  }

  get() {
    return Buffer.concat(this.chunks, this.length);
  }
}

function debug(...logs) {
  if (debug.enabled) {
    console.log(...logs);
  }
}
debug.enabled = false;

let pipelineP = promisify(pipeline);

function runCommand(cmd, args) {
  let proc = spawn(cmd, args);

  return {
    stdin: proc.stdin,
    stdout: proc.stdout,
    stderr: proc.stderr,
    promise: new Promise((resolve, reject) => {
      // Note that both error and exit may be called, but calling resolve/reject more than once is a noop.
      proc.on('error', reject);
      proc.on('exit', resolve);
    })
  };
}

async function runSimpleCommandP(cmd, args) {
  let { stdout, stderr, promise } = runCommand(cmd, args);
  let outbuf = stdout.pipe(new BufferStream());
  let errbuf = stderr.pipe(new BufferStream());
  let rcode = await promise;

  return {
    rcode,
    stdout: outbuf.get().toString(),
    stderr: errbuf.get().toString()
  };
}

// Rejects if rcode === 0.
async function checkRunSimpleCommandP(cmd, args) {
  debug(`Running command '${cmd} ${args.join(' ')}'`);

  let result = await runSimpleCommandP(cmd, args);
  if (result.rcode !== 0) {
    throw new Error(`Command '${cmd} ${args.join(' ')}' failed with rcode=${result.rcode}: ${result.stderr}`);
  }
  return result;
}

async function runJsonCommandP(cmd, args) {
  let { stdout } = await checkRunSimpleCommandP(cmd, args);
  return JSON.parse(stdout);
}

function getRemotePath(remote, path) {
  return `${remote}:${path}`;
}

function getArchivePrefix(name) {
  return `archive-${name}`;
}

async function getBackupsP(remotePath, archivePrefix) {
  let backups = await runJsonCommandP('rclone', ['lsjson', '--hash', remotePath, `--include=${archivePrefix}*`]);
  return backups.map(b => {
    return { ...b, ModTime: moment(b.ModTime) };
  });
}

// Sorts from newest to oldest.
function sortBackupsByModTime(backups) {
  return backups.sort(({ ModTime: a }, { ModTime: b }) => b.diff(a));
}

// Replaces duration specs with moment.duration.
function parseSchedule(schedule) {
  return schedule.map(({ every, for: _for }) => ({
    every: every ? moment.duration(every) : null,
    for: _for ? moment.duration(_for) : null,
  }));
}

function getBackupsToRetain(backups, schedule) {
  let retain = new Map();
  let sortedBackups = sortBackupsByModTime(backups);

  for (let { every: interval, for: period } of parseSchedule(schedule)) {
    period = period || moment.duration({ years: 99 }); // default to a long time
    let intervalStart = moment();

    for (let backup of sortedBackups) {
      // If the backup is older than the retention period, do not retain this nor older backups.
      if (backup.ModTime.isBefore(moment().subtract(period))) {
        break;
      }

      if (intervalStart.isSameOrAfter(backup.ModTime)) {
        retain.set(backup.ID, backup);

        // Use the last backup time as start for next interval since we
        // use last backup time to determine whether a new one is
        // needed. This should make for a more stable set of retained
        // backups than if we just walked backwards at steps of
        // interval from now.
        intervalStart = moment(backup.ModTime).subtract(interval);
      }
    }
  }

  return [...retain.values()];
}

function getBackupsToDelete(backups, schedule) {
  let retainIds = getBackupsToRetain(backups, schedule).map(({ ID }) => ID);
  return backups.filter(({ ID }) => !retainIds.includes(ID));
}

function getShouldBackup(backups, schedule) {
  if (schedule.length === 0) {
    return false
  }

  let sortedBackups = sortBackupsByModTime(backups);
  let sortedSchedules = parseSchedule(schedule).sort(({ every: a }, { every: b }) => a.milliseconds() - b.milliseconds());
  let minInterval = sortedSchedules[0].every;

  return sortedBackups.length === 0 || moment().subtract(minInterval).isAfter(sortedBackups[0].ModTime);
}

async function createBackupP(contents, secretPath, compress, tmpDir) {
  let tmpFile = path.join(tmpDir, crypto.randomBytes(32).toString('hex'));
  let output = fs.createWriteStream(tmpFile);

  let archive = archiver('tar');
  for (let { path, mountPath } of contents) {
    archive.file(path, { name: mountPath });
  }
  archive.finalize();

  // stream to gpg so we don't have to go to disk
  // FIXME: error handling here isn't robust; when I add an invalid flag it crashes on gzip emit
  let { stdin: gpgIn, stdout: gpgOut, stderr: gpgErr, promise: gpgRcodeP } =
      runCommand('gpg', ['--symmetric', '--pinentry-mode', 'loopback', '--passphrase-file', secretPath]);
  let gpgErrBuf = gpgErr.pipe(new BufferStream());
  let size = 0;

  await Promise.all([
    pipelineP(
      archive,
      zlib.createGzip({
        level: compress ? zlib.constants.Z_BEST_COMPRESSION : zlib.constants.Z_NO_COMPRESSION,
      }),
      gpgIn
    ),
    pipelineP(
      gpgOut,
      new PassThrough().on('data', chunk => size += chunk.length),
      output
    )
  ]);
  let gpgRcode = await gpgRcodeP;
  if (gpgRcode !== 0) {
    throw new Error(`GPG encryption failed with rcode=${gpgRcode}: ${gpgErrBuf.get().toString()}`);
  }

  return { filePath: tmpFile, size };
}

async function getContentsP(cspec) {
  let contents = [];
  for (let { base, filePattern, mountBase } of cspec) {
    filePattern = filePattern || '';
    mountBase = mountBase || '';

    let matches = await promisify(glob)(
      path.join(base, filePattern),
      { realpath: true, nodir: true }
    );

    contents.push(
      ...matches.map(match => ({
        path: match,
        mountPath: path.join(mountBase, path.relative(base, match))
      }))
    );
  }
  return contents;
}

async function uploadBackupP(remotePath, archivePrefix, filePath, createTime, tags) {
  let dateSuffix = createTime.format('YYYYMMDDHHmmss');
  let tagsSuffix = Object.entries(tags || {}).map(([tag, value]) => `${tag}=${value}`).join('-');
  tagsSuffix = tagsSuffix ? `-${tagsSuffix}` : '';
  let archiveName = `${archivePrefix}-${dateSuffix}${tagsSuffix}.tar.gz.gpg`;

  await checkRunSimpleCommandP('rclone', ['copyto', filePath, path.join(remotePath, archiveName)]);
}

async function processArchivesP(archives, secretPath, forceBackupArchives, tmpDir) {
  for (let aspec of archives) {
    let archivePrefix = getArchivePrefix(aspec.name);
    let compress = aspec.compress !== false; // compress if true or null

    let backupRemotePaths = [];

    for (let rspec of aspec.remotes) {
      let remotePath = getRemotePath(rspec.name, rspec.path);
      let backups = await getBackupsP(remotePath, archivePrefix);
      console.log(`Found ${backups.length} backup(s) of archive ${aspec.name} at ${remotePath}`);
      debug('Backups: ', backups);

      let backupsToDelete = getBackupsToDelete(backups, rspec.schedule);
      console.log(`Deleting ${backupsToDelete.length} backup(s) of archive ${aspec.name} at ${remotePath}`);
      debug('Deleting backups: ', backupsToDelete);
      for (let { Name } of backupsToDelete) {
        await checkRunSimpleCommandP('rclone', ['deletefile', path.join(remotePath, Name)]);
      }

      if (getShouldBackup(backups, rspec.schedule) || forceBackupArchives.includes(aspec.name)) {
        debug(`Backup needed for ${remotePath}`);
        backupRemotePaths.push(remotePath);
      }
    }

    // FIXME: would it be better to do the upload before delete to avoid data loss?
    if (backupRemotePaths.length === 0) {
      console.log(`No remotes require backup, exiting`);
      return
    }
    debug(`Backups required at remotes: ${backupRemotePaths.join(', ')}`);

    let contents = await getContentsP(aspec.contents);
    let { filePath, size } = await createBackupP(contents, secretPath, compress, tmpDir);
    debug(`Backup of archive ${aspec.name} at ${filePath}: size=${size}b`);
    let createTime = moment();

    // FIXME: if uploading to one remote fails, they probably shouldn't all fail

    // bracket this to ensure backup is cleaned up with finally()
    await (async () => {
      for (let remotePath of backupRemotePaths) {
        console.log(`Uploading backup of archive ${aspec.name} to ${remotePath}`);
        // We pass in the create time so the same archive on different remotes has the same file name.
        await uploadBackupP(remotePath, archivePrefix, filePath, createTime);
      }
    })()
      .finally(() => {
        debug(`Deleting temp archive at ${filePath}`);
        return fs.promises.unlink(filePath);
      });
  }
}


return (async () => {
  let args =
    require('yargs')
      .option('archives', {
        desc: 'Path to the archive descriptor file',
        demandOption: true,
        type: 'string'
      })
      .options('secret', {
        desc: 'Path to the encryption key file',
        demandOption: true,
        type: 'string'
      })
      .option('tmp', {
        desc: 'Directory to store temporary files',
        demandOption: true,
        type: 'string'
      })
      .option('force', {
        desc: 'Creates a backup for the given archives even if they are not scheduled for one',
        default: [],
        type: 'array'
      })
      .option('debug', {
        default: false,
        type: 'boolean'
      })
      .strict(true)
      .argv;
  let archives = JSON.parse(await fs.promises.readFile(args.archives, { encoding: 'utf8' }));

  debug.enabled = args.debug;

  await processArchivesP(archives, args.secret, args.force, args.tmp);

  console.log('Done');
})();
