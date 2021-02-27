/**
 * Become an SFTP server that uses Google Cloud Storage as the back-end storage
 * system.  The applications uses the SSH2 Node.js package which provides a high level implementation of the
 * SFTP protocol as documented here: https://tools.ietf.org/html/draft-ietf-secsh-filexfer-02
 * 
 * The Google Cloud Storage (GCS) environment provides a blob storage container (bucket) holding blobs of data (files).
 * 
 */
const util = require('util');
const {Storage} = require('@google-cloud/storage');
const fs = require('fs');
const crypto = require('crypto');
const ssh2 = require('ssh2');
const {SFTPStream} = require('ssh2-streams');
const PATH = require('path');
const winston = require('winston');
const {format} = winston;

// Imports the Google Cloud client library for Winston
const {LoggingWinston} = require('@google-cloud/logging-winston');
const { dir } = require('console');

const loggingWinston = new LoggingWinston({
    "logName": "sftp-gcs"
});
const myFormat = format.printf( ({ level, message, timestamp }) => {
    return `${timestamp} ${level}: ${message}`;
});

const STATUS_CODE = ssh2.SFTP_STATUS_CODE;

const MODE_FILE = fs.constants.S_IFREG | fs.constants.S_IRWXU | fs.constants.S_IRWXG | fs.constants.S_IRWXO;
const MODE_DIR = fs.constants.S_IFDIR | fs.constants.S_IRWXU | fs.constants.S_IRWXG | fs.constants.S_IRWXO;

// Parse the arguments
// * --bucket BUCKET_NAME (required)
// * --port PORT_NUMBER (default: 22)
// * --user USERID
// * --password PASSWORD
// * --public-key-file PUBLIC-KEY file
// * --service-account-key-file KEY_FILE
//
const argv = require('yargs')
    .usage('$0 --bucket BUCKET_NAME [--port PORT_NUM] [--user USER_NAME] [--password PASSWORD]')
    .options('bucket').describe('bucket','GCS bucket to work with').string('bucket').nargs('bucket', 1).demandOption('bucket', 'Must supply a GCS bucket')
    .options('port').describe('port', 'Port number to listen upon').number('port').nargs('port', 1).default('port', 22, 'Default port is 22')
    .options('user').describe('user', 'Userid for SFTP client').string('user').nargs('user', 1).default('user', '')
    .options('password').describe('password', 'Password for SFTP client').string('password').nargs('password', 1).default('password', '')
    .options('public-key-file').describe('public-key-file', 'Publish SSH key').string('public-key-file').nargs('public-key-file', 1).default('public-key-file', '')
    .options('service-account-key-file').describe('service-account-key-file', 'Key file for service account').string('service-account-key-file').nargs('service-account-key-file', 1).default('service-account-key-file', '')
    .options('debug').describe('debug', 'Set the debugging log level').string('debug').nargs('debug', 1).default('info', '')
    .argv;

// Uncomment the following to log the passed in arguments.
// console.log(`args: ${util.inspect(argv)}`);

// Create a Winston logger that streams to Stackdriver Logging
// Logs will be written to: "projects/YOUR_PROJECT_ID/logs/winston_log"
const logger = winston.createLogger({
    level: argv.debug,
    transports: [
      new winston.transports.Console(),
      // Add Cloud Logging Logging
      loggingWinston,
    ],
    format: format.combine(
      format.label({ label: 'sftp-gcs', message: true }),
      format.timestamp(),
      myFormat
    )
  });
/*
ATTRS
An object with the following valid properties:

* mode - integer - Mode/permissions for the resource.
* uid - integer - User ID of the resource.
* gid - integer - Group ID of the resource.
* size - integer - Resource size in bytes.
* atime - integer - UNIX timestamp of the access time of the resource.
* mtime - integer - UNIX timestamp of the modified time of the resource.
*/

const allowedUser = argv.user;
const allowedPassword = argv.password;
const serviceAccountKeyFile = argv["service-account-key-file"];
const publicKeyFile = argv["public-key-file"];
const SERVER_PORT = argv.port

// If the byucket name begins with "gs://", remove it.
const BUCKET_NAME = argv.bucket.replace(/gs:\/\//,'');

let allowedPubKey = null;
if (publicKeyFile !== "") {
    allowedPubKey = ssh2.utils.parseKey(fs.readFileSync(publicKeyFile));
}

const storageOptions = {};
if (serviceAccountKeyFile.length !== "") {
    storageOptions.keyFilename = serviceAccountKeyFile;
}
const storage = new Storage(storageOptions); // Get access to the GCS environment
const bucket = storage.bucket(BUCKET_NAME); // The bucket is a global and provides access to the GCS data.

/**
 * We need a host key to be used to identify our server.  We will look for such a key in a few places.
 * 
 * Our algorithm will be:
 * 
 * ---
 * if (/etc/ssh/ssh_host_rsa_key exists) {
 *   if (we can read the file) {
 *     Return the content as a host key
 *   }
 *   Warn that we found the file but could not read it.
 * }
 * Warn that we are going to use a default host key
 * return the common host key.
 * ---
 */
function getHostKey() {
    if (fs.existsSync('/etc/ssh/ssh_host_rsa_key')) {
        try {
            return fs.readFileSync('/etc/ssh/ssh_host_rsa_key')
        }
        catch(err) {
            logger.warn(`Unable to read /etc/ssh/ssh_host_rsa_key even though it exists.`);
        }
    }
    logger.warn(`Unable to find/access a system host key, using the application default host key.`);
    return fs.readFileSync('keys/default_host.key');
} // getHostKey


/**
 * We are passed paths that represent the SFTP client's vision of a path that is distinct from that of
 * GCS.  We have to perform processing on the path to bring it to a canonical format.  This includes
 * handling of prefix for the root of a file system ('xxx' vs '/xxx') and handling of relative
 * directories such as '.' and '..'
 * @param {*} path The path to be processed.
 */
function normalizePath(path) {
    const start = path;
    // If path starts with '/', remove '/'
    if (path.startsWith('/')) {
        path = path.substring(1);
    }
    if (path.endsWith('.')) {
        path = path.substring(0, path.length - 1);
    }
    path = PATH.normalize(path);
    if (path === '.') {
        path = '';
    }
    if (path === '..') {
        path = '';
    }
    logger.debug(`Converted "${start}" to "${path}"`)
    return path;
} // End normalizePath


/**
 * Get the stat data of a file or directory.
 * @param {*} path 
 * @returns An object describing the attributes of the thing at the path or null if no such object
 * can be found.
 */
async function getStatData(path) {
    if (path === "/") { // The root is a directory ... simple base/special case.
        const attrs = {
            "mode": MODE_DIR
        };
        return attrs;
    }
    path = normalizePath(path);
    try {

        // We test to see if we have a file of the exact name.  If yes, then use it's attributes.
        let [exists] = await bucket.file(path).exists();
        if (exists) {
            const [metadata] = await bucket.file(path).getMetadata();

            const attrs = {
                "mode": MODE_FILE,
                "size": Number(metadata.size)
            };
            return attrs;
        }

        // We don't have an exact name match now we look to see if we have a file with this as a prefix.
        const [fileList] = await bucket.getFiles({
            "delimiter": '/',
            "directory": path,
            "autoPaginate": false
        });
        if (fileList.length == 0) {
            logger.debug(`Could not find ${path}`);
            return null;
        }
        logger.debug(`"${path}" is a directory!`)
        const attrs = {
            "mode": MODE_DIR
        };
        return attrs;
    }
    catch(exc) {
        logger.debug(`STAT Error: ${exc}`);
        return null;
    }    
    return null;
} // getStatData


// Create a string representation of a long directory listing entry (ls -l).  This is composed of file name, is it a directory, size of the file and padding.
function fileLongEntry(name, isDirectory, size, padding, created) {
    if (isDirectory) {
        size = 0;
    }
    return `${isDirectory?"d":"-"}rw-rw-rw- 1 none none ${String(size).padStart(padding)} ${created} ${name}`;
} // fileLongEntry


new ssh2.Server({
    hostKeys: [getHostKey()],
    "greeting": "SFTP-GCS demon",
    "banner": "SFTP-GCS demon"
}, function (client) {
    logger.debug('Client connected!');

    // ctx.username - The identity of the user asking to be authenticated,
    // ctx.method - How is the request being asked to authenticate?
    // - password - ctx.password contains the password.
    // - publickey - ctx.key, ctx.signature, ctx.blob
    //
    // We must either call ctx.reject() or ctx.accept()
    //
    client.on('authentication', function (ctx) {
        logger.debug(`authentication: method=${ctx.method}`);
        //var user = Buffer.from(ctx.username);
        switch (ctx.method) {
            case 'none':
                if (allowedUser.length !== 0) {
                    logger.debug(`We have at least a user to match`);
                    return ctx.reject(['password', 'publickey'], true);
                }
                if (allowedPassword.length !== 0) {
                    logger.debug(`We have at least a password to match`);
                    return ctx.reject(['password', 'publickey'], true);
                }
                if (allowedPubKey !== null) {
                    logger.debug(`We want a public key exchange`);
                    // The following code lifted as a solution to this issue:
                    // https://github.com/mscdex/ssh2/issues/235 for SSH2.
                    return ctx.reject(['password', 'publickey'], true);
                }
                return ctx.accept(); // No userid and no password and no public key, come on in!

            case 'password':
                // If a username was supplied when the sftp-gcs app was started and the supplied sftp client
                // username does not match then we can't authenticate.
                if (allowedUser.length > 0 && allowedUser !== ctx.username) {
                    logger.debug(`usernames did not match`);
                    return ctx.reject();
                }

                // If a password was supplied with the sftp-gcs app was started and the supplied sftp client
                // password does not match then we can't authenticate.
                if (allowedPassword.length > 0 && allowedPassword !== ctx.password) {
                    logger.debug(`password did not match`)
                    return ctx.reject();
                }

                return ctx.accept();

            case 'publickey':
                logger.debug(`key: ${util.inspect(ctx.key)}, signature: ${util.inspect(ctx.signature)}`);

                if (allowedPubKey === null) {
                    logger.debug(`No PubKey`);
                    return ctx.reject();
                }
                var allowedPubSSHKey = allowedPubKey.getPublicSSH();
                if (ctx.key.algo !== allowedPubKey.type
                   || ctx.key.data.length !== allowedPubSSHKey.length
                   || !crypto.timingSafeEqual(ctx.key.data, allowedPubSSHKey)
                   || (ctx.signature && allowedPubKey.verify(ctx.blob, ctx.signature) !== true)) {
                    logger.debug(`Rejected login`);
                    return ctx.reject();
                }
                logger.debug(`SSH key allowed login`);
                return ctx.accept();

            default:
                return ctx.reject();
        }

        ctx.reject(); // We should never reach here!!
    }); // End on authentication

    client.on('ready', function () {
        logger.debug('Client authenticated!');

        client.on('session', function (accept, reject) {
            const session = accept();
            session.on('sftp', function (accept, reject) {
                logger.debug('Client SFTP session');
                const openFiles = new Map(); // The map of open files.

                // Get the file record (the open file) from the set of open files based on the value contained in the
                // handle buffer.  This function either returns a fileRecord object or null if no corresponding file 
                // record object can be found.
                function getFileRecord(handleBuffer) {
                    // Validate that the handle buffer is the right size for a 32bit BE integer.
                    if (handleBuffer.length !== 4) {
                        logger.debug("ERROR: Buffer wrong size for 32bit BE integer");
                        return null;
                    }

                    const handle = handleBuffer.readUInt32BE(0); // Get the handle of the file from the SFTP client.
                    
                    if (!openFiles.has(handle)) {
                        logger.debug(`Unable to find file with handle ${handle}`);
                        return null;
                    }

                    const fileRecord = openFiles.get(handle);
                    return fileRecord;
                } // getFileRecord

                let handleCount = 0;
                // `sftpStream` is an `SFTPStream` instance in server mode
                // see: https://github.com/mscdex/ssh2-streams/blob/master/SFTPStream.md
                const sftpStream = accept();

                async function commonStat(reqId, path) {
                    const attrs = await getStatData(path);
                    if (attrs === null) {
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                    sftpStream.attrs(reqId, attrs);  
                } // End of commonStat

                //
                // Handle OPEN
                //
                // "flags" is a bitfield containing any of the flags defined in SFTPStream.OPEN_MODE. These modes are:
                //
                // o READ
                // o WRITE
                // o APPEND
                // o CREAT
                // o TRUNC
                // o EXCL
                //
                sftpStream.on('OPEN', async function (reqId, filename, flags, attrs) {
                    logger.debug(`OPEN<${reqId}>: filename: "${filename}", flags: ${SFTPStream.flagsToString(flags)}`)

                    filename = normalizePath(filename);
                    
                    const handle = handleCount;
                    handleCount = handleCount + 1;
                    const fileRecord = {
                        "handle": handle,
                        "path": filename
                    };
                    
                    if (flags & SFTPStream.OPEN_MODE.WRITE) {
                        logger.debug('Opening file for WRITE');

                        // We now need to open the GCS file for writing.  It will be written in subsequent WRITE requests.
                        fileRecord.gcsFile = bucket.file(filename);
                        fileRecord.gcsError = false;
                        fileRecord.writeStream = fileRecord.gcsFile.createWriteStream();
                        fileRecord.writeStream.on('error', (err) => {
                            logger.debug(`Detected an error with writeStream to the GCS file: ${err}`);
                            fileRecord.gcsError = true;
                        });
                        // End of WRITE
                    } else if (flags & SFTPStream.OPEN_MODE.READ) {
/**
 * Handling reads for SFTP mapped to GCS is an interesting story.  SFTP assumes block oriented access to the files that it thinks it is
 * reading.  SFTP clients send individual requests to read chunks of storage  For example, an SFTP client may send a request such as:
 * 
 * READ: reqId: 12, offset: 2048, maxLength: 512
 * 
 * This would be interpreted as read and return up to 512 bytes starting at offset 2048 into the file.  With GCS access, we retrieve our data
 * as a stream of data starting from the beginning.  We have no way to ask GCS to obtain an arbitrary block.  We might think we can simply map the
 * SFTP requests to serial consumption of the stream data but there are dangers in that story.  First of all, SFTP doesn't require that block
 * read requests arrive one at a time or in order.  For example, the following two request messages may very well arrive:
 * 
 * READ: reqId: x, offset: 2048, maxLength: 512
 * READ: reqId: y, offset: 0, maxLength: 1024
 * 
 * We may easily get a request for a block that comes later in the stream.  We can assume that we will eventually get requests for all blocks but
 * must not assume that they come in order.  We can't simply process a read request with the next chunk of data read from the GCS object.  We
 * should also note that we may get multiple READ requests before any previous ones have been satisfied.  Our SFTP server much honor the contract
 * and not make assumptions on the order of the client send requests.
 * 
 * Our algorithm is to receive READ requests and place them in a Map() keyed by the offset start of the data.  From the GCS stream, we know what the
 * offset of the incoming data is.  To be specific, when we start a new GCS stream, it is at offset 0.  As we consume data of length "n" from the
 * stream, the next offset moves forward by "n".  This then gives us the notion of matching up READ requests for data at a given starting offset
 * and data arriving from the GCS stream.  Whenever a new READ request arrives, we add it to the map and then "process the map".  We perform
 * the following:
 * 
 * From the GCS stream we have a current "offset".  Do we have a READ request which starts at that offset?  If yes, we can return data.
 * If no, we can not return data and must await some expected future READ request which does start at that offset.  When ever a new READ
 * request arrives, we again perform this processing.  One further twist is that we don't want to return only the available data but instead
 * we want to maximize the data returned in a READ request.  Let us look at an example.  Imagine we have a READ request that asks for data
 * at offset 0 and a maximum length of 32768.  Now consider that from the GCS stream, we may have received 4096 bytes starting at offset 0.
 * We could immediately satisfy the READ and return 4096 bytes.  This would be legal as per the SFTP spec but it would not be optimal.  Instead
 * we want to wait until we have 32768 bytes (or more) to return in the READ request.  Adding this twist to our story means that we aren't
 * just looking for some data, we are looking for as much data as possible.
 */
                        logger.debug(`Opening file for READ`);
                        fileRecord.gcsError = false;
                        let gcsEnd = false;
                        let gcsOffset = 0;                        
                        let activeRead = null;
                        const readMap = new Map();
                        fileRecord.getGCSData = function(offset, requestedLength) {
                            return new Promise((resolve, reject) => {
                                readMap.set(offset, {
                                    "offset": offset,
                                    "requestedLength": requestedLength,
                                    "resolve": resolve,
                                    "reject": reject
                                });
                                fileRecord.processQueue();
                            });
                        }; // End of getData()
                        const gcsStream = bucket.file(filename).createReadStream();

                        gcsStream.on('error', (err) => {
                            logger.debug(`GCS readStream: Error: ${err}`);
                            fileRecord.gcsError = true;
                        });
                        gcsStream.on('end', () => {
                            logger.debug(`End of GCS stream`);
                            gcsEnd = true;
                        });
                        gcsStream.on('readable', () => {
                            fileRecord.processQueue();
                        });
                        fileRecord.processQueue = function() {
// If we have been asked to process the waiting for data queue and we have reached the EOF of the GCS stream
// then the requests will never be fulfilled.  Resolve each of them with null indicating that we have no data.
                            if (gcsEnd) {
                                readMap.forEach((entry) => {
                                    readMap.delete(entry.offset);
                                    entry.resolve(null);
                                });
                                return;
                            }
                            if (fileRecord.gcsError) {
                                readMap.forEach((entry) => {
                                    readMap.delete(entry.offset);
                                    entry.reject(null);
                                });
                                return;
                            }
                            while(true) {
                                if (activeRead) {
                                    const data = gcsStream.read(activeRead.requestedLength);
                                    if (data === null) {
                                        return;
                                    }
                                    activeRead.resolve(data);
                                    readMap.delete(activeRead.offset);
                                    activeRead = null;
                                    gcsOffset += data.length;
                                }
                                if (!readMap.has(gcsOffset)) {
                                    return;
                                }
                                activeRead = readMap.get(gcsOffset);
                            } // End while true
                        } // End processQueue
                    } // End READ                                                              
                    else {
                        logger.debug(`Open mode not supported`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }

                    openFiles.set(handle, fileRecord); // We are indeed going to process opening a file ... so record the fileRecord.

                    // Return the file handle in BigEndian format as unsigned 32bit.
                    const handleBuffer = Buffer.alloc(4);
                    handleBuffer.writeUInt32BE(handle, 0);
                    sftpStream.handle(reqId, handleBuffer);
                }); // End handle OPEN


                //
                // Handle WRITE
                //
                // Called when the client sends a block of data to be written to the
                // file on the SFTP server.
                //
                sftpStream.on('WRITE', function (reqId, handleBuffer, offset, data) {
                    // WRITE(< integer >reqID, < Buffer >handle, < integer >offset, < Buffer >data)

                    const fileRecord = getFileRecord(handleBuffer);
                    if (fileRecord === null) { return sftpStream.status(reqId, STATUS_CODE.FAILURE); }

                    logger.debug(`WRITE<${reqId}>: handle: ${fileRecord.handle}, offset ${offset}: data.length=${data.length}`);

                    if (fileRecord.gcsError === true) {
                        logger.debug(`Returning failure in WRITE because of flagged gcsError`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }

                    fileRecord.writeStream.write(data, () => {
                        //logger.debug(`<${reqId}>: Write completed`);
                        sftpStream.status(reqId, STATUS_CODE.OK);
                    });                    
                }); // End handle WRITE


                // Handle a SFTP protocol read request.  We are asked to get data starting at a given offset for
                // a maximum requested length.  The outcome will either be data or a status of EOF.
                sftpStream.on('READ', async function(reqId, handleBuffer, offset, requestedLength){
                    // READ(< integer >reqID, < Buffer >handle, < integer >offset, < integer >length)

                    const fileRecord = getFileRecord(handleBuffer);
                    if (fileRecord === null) { return sftpStream.status(reqId, STATUS_CODE.FAILURE); }

                    logger.debug(`READ<${reqId}>: handle: ${fileRecord.handle}, offset: ${offset}, max length: ${requestedLength}`);

                    // Request GCS data starting at a given offset for a requested length.  This is a promise that will
                    // be eventually fulfilled.  The data returned is either a Buffer or null.  If null, that is the
                    // indication that we have reached the end of file.
                    fileRecord.currentReqid = reqId;
                    try {
                        const data = await fileRecord.getGCSData(offset, requestedLength);
                        if (data === null) {  // If we get null, we have reached the end of file.
                            return sftpStream.status(reqId, STATUS_CODE.EOF);
                        }
                        return sftpStream.data(reqId, data); // Return the requested data.
                    } catch(ex) {
                        logger.debug(`Exception: ${util.inspect(ex)}`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                }); // End handle READ

        
                sftpStream.on('MKDIR', async function(reqId, path, attrs) {
                    // MKDIR(< integer >reqID, < string >path, < ATTRS >attrs)
                    logger.debug(`MKDIR<${reqId}>: path: "${path}", attrs: ${util.inspect(attrs)}`);
                    try {
                        path = normalizePath(path);
                        const dirName = path + "/";
                        const [exists] = await bucket.file(dirName).exists();
                        if (exists) {
                            logger.debug(`something called ${dirName} already exists`);
                            return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        }
                        // Create a stream and then immediately end writing to it. This creates a zero length file.
                        const stream = bucket.file(dirName).createWriteStream();
                        stream.end(()=>{
                            sftpStream.status(reqId, STATUS_CODE.OK);
                        });
                    } catch(ex) {
                        logger.debug(`Exception: ${util.inspect(ex)}`);
                        sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                }); // End handle MKDIR


                /**
                 * Handle the SFTP OPENDIR request.
                 * * reqId is the request identifier that is returned in a matching response.
                 * * path is the directory that we are going to list
                 */
                sftpStream.on('OPENDIR', async function(reqId, path) {
                    // OPENDIR(< integer >reqID, < string >path)
                    
                    logger.debug(`OPENDIR<${reqId}> path: "${path}"`);
                    path = normalizePath(path);
                    // Check that we have a directory to list.
                    if (path !== "") {
                        // Return an error
                        // We have handled the simplest case, now we need to see if a directory exists with this name. Imagine we have been 
                        // asked to open "/dir".  This will have been normalized to "dir".  From a GCS perspective, we now want to determine if there are any files
                        // that begin with the prefix "dir/".  If yes, then the directory exists.
                        try {
                            const [fileList] = await bucket.getFiles({
                                "directory": path,
                                "delimiter": "/",
                                "autoPaginate": false
                            });
                            if (fileList.length == 0) {
                                logger.debug(`we found no files/directories with directory: "${path}"`);
                                return sftpStream.status(reqId, STATUS_CODE.NO_SUCH_FILE);
                            }
                        } catch(ex) {
                            logger.debug(`Exception: ${util.inspect(ex)}`);
                            return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        }
                    }

                    const handle = handleCount;
                    handleCount = handleCount + 1;

                    const fileRecord = {
                        "handle": handle,
                        "path": path,
                        "readComplete": false // Have we completed our reading of data.
                    };

                    openFiles.set(handle, fileRecord);
                    const handleBuffer = Buffer.alloc(4);
                    handleBuffer.writeUInt32BE(handle, 0);
                    sftpStream.handle(reqId, handleBuffer);
                }); // End handle OPENDIR


                sftpStream.on('READDIR', async function(reqId, handleBuffer) {
                    //
                    // READDIR will be called multiple rimes following an OPENDIR until the READDIR
                    // indicated that we have reached EOF.  The READDIR will return an array of
                    // directory objects where each object contains:
                    // {
                    //   filename:
                    //   longname:
                    //   attrs:
                    // }
                    //
                    // READDIR(< integer >reqID, < Buffer >handle)
                    //

                    const fileRecord = getFileRecord(handleBuffer);
                    if (fileRecord === null) { return sftpStream.status(reqId, STATUS_CODE.FAILURE); }

                    logger.debug(`READDIR<${reqId}>: handle: ${fileRecord.handle}, path: "${fileRecord.path}"`);

                    // When READDIR is called, it is expected to return some (maybe all) of the files in the directory.
                    // It has two return values ... either one or more directory entries or an end of file marker indicating
                    // that all the directory entries have been sent.  In our GCP mapping, on the first call, we return
                    // all the directory entries and on the second call we return an EOF marker.  This satisfies the contract.
                    // After the first call, we set a flag that indicates that the read of the directory is complete and that
                    // subsequent calls should return EOF.
                    if (fileRecord.readComplete) {
                        return sftpStream.status(reqId, STATUS_CODE.EOF);
                    }

                    fileRecord.readComplete = true;

                    bucket.getFiles({
                        "autoPaginate": false,
                        "delimiter": '/',
                        "directory": fileRecord.path,
                        "includeTrailingDelimiter": true
                    }, (err, fileList, nextQuery, apiResponse) => {
// The responses from a GCS file list are two parts.  One part is files in the current "directory" while the other part is the
// list of directories.  This is of course fake as GCS has no concept of directories.
//
                        if (err) {
                            logger.debug(`Err: ${util.inspect(err)}`);
                            return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        }
                        const results = [];

                        // Find the largest file size ...  We then determine how many characters this is and then
                        // this becomes the padding for the long entry listing.
                        let largest = 0;

                        fileList.forEach((file) => {
                            if (Number(file.metadata.size) > largest) {
                                largest = file.metadata.size;
                            }
                        });
                        const padding = String(largest).length;


                        const dirPath = fileRecord.path + "/";
                        fileList.forEach((file) => {
                            /*
                            if (file.name.endsWith('/')) {
                                return;
                            }
                            */
                            //const name = file.name.replace(/.*\//,'');
                            let isDirectory = false;
                            let name = file.name;
                            //logger.debug(`file name: ${util.inspect(name)}`);

                            if (name.startsWith(dirPath)) {
                                name = name.substring(dirPath.length);
                            }
                            //logger.debug(`file name2: ${util.inspect(name)}`);

                            // Remove prefix

                            if (name.endsWith('/')) {
                                name = name.substr(0, name.length-1);
                                isDirectory = true;
                            }

                            if (name === '') {
                                return;
                            }

                            //logger.debug(`Metadata: ${util.inspect(file.metadata)}`);
                           

// mode  - integer - Mode/permissions for the resource.
// uid   - integer - User ID of the resource.    
// gid   - integer - Group ID of the resource.                         
// size  - integer - Resource size in bytes.                         
// atime - integer - UNIX timestamp of the access time of the resource.
// mtime - integer - UNIX timestamp of the modified time of the resource.                            
                            const newNameRecord = {
                                "filename": name,
                                "longname": fileLongEntry(name, isDirectory, file.metadata.size, padding, new Date(file.metadata.timeCreated).toISOString()),
                                "attrs": {
                                    "mode": isDirectory?MODE_DIR:MODE_FILE,
                                    "size": Number(file.metadata.size),
                                    "atime": 0,
                                    "mtime": new Date(file.metadata.updated).getTime()/1000
                                }
                            };
                            results.push(newNameRecord);
                            //logger.debug(util.inspect(newNameRecord));
                        });
                        
                        /*
                        if (apiResponse.prefixes) {
                            apiResponse.prefixes.forEach((filePrefix) => {
                                let fileName = filePrefix.substring(0, filePrefix.length-1);
                                fileName = fileName.replace(/.*\//,'');
                                results.push({
                                    "filename": fileName,
                                    "longname": fileLongEntry(fileName, true, 0, padding, ""),
                                    "attrs": {
                                        "size": 0,
                                        "atime": 0,
                                        "mtime": 0
                                    }
                                });
                            });
                        }
                        */
                        
                        
                        fileRecord.readComplete = true; // Flag that a subseqent call should return EOF.
                        return sftpStream.name(reqId, results);
                    });
                   
                }); // End handle READDIR


                sftpStream.on('LSTAT', async function(reqId, path) {
                    // LSTAT(< integer >reqID, < string >path)
                    // use attrs() to send attributes of the requested file back to the client.
                    logger.debug(`LSTAT<${reqId}>: path: "${path}"`);
                    commonStat(reqId, path);
                }); // End handle LSTAT
                

                sftpStream.on('STAT', async function(reqId, path) {
                    // STAT(< integer >reqID, < string >path)
                    logger.debug(`STAT<${reqId}>: path: "${path}"`);
                    commonStat(reqId, path);
                });


                sftpStream.on('FSTAT', async function(reqId, handleBuffer) {
                    // FSTAT(< integer >reqID, < Buffer >handle)
                    const fileRecord = getFileRecord(handleBuffer);
                    if (fileRecord === null) { return sftpStream.status(reqId, STATUS_CODE.FAILURE); }

                    logger.debug(`FSTAT<${reqId}>: handle: ${fileRecord.handle} => path: "${fileRecord.path}"`);
                    if (!fileRecord.path) {
                        logger.error("Internal error: FSTAT - no path in fileRecord!");
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                    commonStat(reqId, fileRecord.path);
                }); // End handle FSTAT


                sftpStream.on('SETSTAT', function(reqId, path, attrs) {
                    // SETSTAT < integer >reqID, < string >path, < ATTRS >attrs)
                    logger.debug(`SETSTAT<${reqId}>: path: "${path}", attrs: ${util.inspect(attrs)}`);
                    // Although we don't actually set any attributes, we say that we did.  WinSCP seems to complain
                    // if we say we didn't.
                    return sftpStream.status(reqId, STATUS_CODE.OK);
                }); // End handle FSETSTAT

                sftpStream.on('FSETSTAT', function(reqId, handleBuffer, attrs) {
                    // FSETSTAT(< integer >reqID, < Buffer >handle, < ATTRS >attrs)
                    logger.debug(`FSETSTAT<${reqId}>`);
                    return sftpStream.status(reqId, STATUS_CODE.OP_UNSUPPORTED);
                }); // End handle FSETSTAT
                

                sftpStream.on('RENAME', async function(reqId, oldPath, newPath) {
                    // RENAME(< integer >reqID, < string >oldPath, < string >newPath)
                    logger.debug(`RENAME<${reqId}>: oldPath: ${oldPath}, newPath: ${newPath}`);
                    oldPath = normalizePath(oldPath);
                    newPath = normalizePath(newPath);
                    // Map the request to a GCS command to rename a GCS object.
                    try {
                        await bucket.file(oldPath).rename(newPath);
                        return sftpStream.status(reqId, STATUS_CODE.OK);
                    } catch(exc) {
                        logger.debug(`Exception: ${util.inspect(exc)}`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                }); // End of handle RENAME


                sftpStream.on('REMOVE', async function(reqId, path) {
                    // REMOVE(< integer >reqID, < string >path)
                    logger.debug(`REMOVE<${reqId}>: path: "${path}"`);
                    path = normalizePath(path);
                    if (path.endsWith('/')) { // Sneaky user trying to remove a directory as though it were a file!
                        logger.debug(`Can't remove a file ending with "/"`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                    // Map the request to a GCS command to delete/remove a GCS object.
                    try {
                        await bucket.file(path).delete();
                        return sftpStream.status(reqId, STATUS_CODE.OK);
                    }
                    catch(exc) {
                        logger.debug(`Failed to delete file "${path}"`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                }); // End handle REMOVE


                /**
                 * Remove the directory named by path.
                 */
                sftpStream.on('RMDIR', async function(reqId, path) {
                    // RMDIR(< integer >reqID, < string >path)
//
// We need to check that the path exists and that it is a directory.
// Imagine a request to delete a directory called "mydir".  We have a number of
// known possibilities:
//
// 1. There is a gcs object called mydir.  This should not be deleted.  Return an error.
// 2. There is a gcs object called mydir/.  This is indeed the directory and should be deleted BUT only ... if there are no objects that contain
//    the mydir/ as a prefix.  If there are, then the directory can not be considered to be empty.
// 3. There is no gcs object called mydir/ but there are objects that are prefixed mydir/.  We should not delete and return an error.  This would be an indication
//    That there is logically a directory called mydir but that it is not empty.
// 4. Otherwise we fail the directory deletion request.
//
                    logger.debug(`RMDIR<${reqId}>: path: "${path}"`);
                    path = normalizePath(path);
                    // If the path does NOT end in with a '/', then add one
                    if (!path.endsWith('/')) {
                        path = path + "/";
                    }

                    try {
                        // Let us see if we have files that end in path:
                        const [fileList] = await bucket.getFiles({
                            "autoPaginate": false,
                            "delimiter": "/",
                            "prefix": path,
                            "maxResults": 2
                        });
                        if (fileList.length == 0) {
                            logger.debug(`No such file/directory: "${path}"`);
                            return sftpStream.status(reqId, STATUS_CODE.NO_SUCH_FILE);
                        }
                        if (fileList.length > 1) {
                            logger.debug(`Directory not empty: "${path}"`);
                            return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        }

                        const [deleteResponse] = await bucket.file(path).delete();
                        return sftpStream.status(reqId, STATUS_CODE.OK);
                    } catch(ex) {
                        logger.debug(`Exception: ${util.inspect(ex)}`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                }); // End handle RMDIR

                
                //
                // Handle CLOSE
                //
                // Called when the client closes the file.  For example at the end of a write.
                // The points where we know a close will be called include following an OPEN and an OPENDIR.
                //
                sftpStream.on('CLOSE', function (reqId, handleBuffer) {

                    const fileRecord = getFileRecord(handleBuffer);
                    if (fileRecord === null) { return sftpStream.status(reqId, STATUS_CODE.FAILURE); }

                    logger.debug(`CLOSE<${reqId}>: handle: ${fileRecord.handle}`);
                    
                    // Close the GCS file stream by calling end().  We save the SFTP request id in the fileRecord.  Notice
                    // that we don't flag the status of this stream request.  Instead, we assume that the call to end will result
                    // in a call to close() which will close the stream and THAT will send back the stream response.
                    openFiles.delete(fileRecord.handle);

                    if (fileRecord.writeStream) {
                        logger.debug(`Closing GCS write stream`);
                        if (fileRecord.gcsError) {
                            logger.debug(`Returning error because of previous GCS Error with write stream`);
                            return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        }
                        fileRecord.writeStream.end(() => {                            
                            sftpStream.status(reqId, STATUS_CODE.OK);
                        });
                        return;
                    }
                    return sftpStream.status(reqId, STATUS_CODE.OK);                    
                }); // End handle CLOSE

                //
                // Handle REALPATH
                //
                // Called when the client wants to know the full path.
                //
                sftpStream.on('REALPATH', function (reqId, path) {                
                    logger.debug(`REALPATH<${reqId}>: path: "${path}"`);
                    path = PATH.normalize(path);
                    if (path === '..') {
                        path = '/';
                    }
                    if (path === '.') {
                        path = '/';
                    }
                    logger.debug(`Returning "${path}"`);
                    sftpStream.name(reqId, [{ filename: path }]);
                }); // End handle REALPATH
            }); // End on sftp
        }); // End on session
    }); // End on ready
    
    client.on('end', function () {
        logger.debug('Client disconnected');
    });
    client.on('error', (err) => {
        logger.debug(`ERROR(client.on): ${util.inspect(err)}`);
    });
}).listen(SERVER_PORT, '0.0.0.0', function () {
    logger.info("****************************************");
    logger.info("*** Google Cloud Storage SFTP Server ***");
    logger.info("****************************************");
    logger.info(`Using bucket: gs://${BUCKET_NAME}`);
    logger.info('Listening on port ' + this.address().port);
    logger.info(`Username: ${allowedUser === ''?'Not set':allowedUser}`);
    logger.info(`Password: ${allowedPassword === ''?'Not set':'********'}`);
    logger.info(`Public key file: ${publicKeyFile===''?'Not set':publicKeyFile}`);
    logger.info(`Service account key file: ${serviceAccountKeyFile===''?'Not set':serviceAccountKeyFile}`);
}).on('error', (err) => {
    // Capture any networking exception.  A common error is that we are asking the sftp-gcs server
    // to listen on a port that is already in use.  Check for that error and call it out specifically.
    logger.info(`Error with networking ${util.inspect(err)}`);
    if (err.code === 'EACCES') {
        logger.info(`It is likely that an application is already listening on port ${err.port}.`);
    }
});
