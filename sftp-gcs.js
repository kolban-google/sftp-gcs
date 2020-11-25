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
const { COPYFILE_EXCL } = require('constants');
const PATH = require('path');

const STATUS_CODE = ssh2.SFTP_STATUS_CODE;

const MODE = fs.constants.S_IFREG | fs.constants.S_IRWXU | fs.constants.S_IRWXG | fs.constants.S_IRWXO;
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
    .argv;

// Uncomment the following to log the passed in arguments.
// console.log(`args: ${util.inspect(argv)}`);


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
const BUCKET_NAME = argv.bucket;

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
    console.log(`Converted ${start} to ${path}`)
    return path;
} // End normalizePath


/**
 * Get the stat data of a file or directory.
 * @param {*} path 
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
        let [exists] = await bucket.file(path).exists();
        if (exists) {
            const [metadata] = await bucket.file(path).getMetadata();

            const attrs = {
                "mode": MODE,
                "size": Number(metadata.size)
            };
            return attrs;
        }

        const [fileList] = await bucket.getFiles({
            "delimiter": '/',
            "directory": path,
            "autoPaginate": false
        });
        if (fileList.length == 0) {
            console.log(`Could not find ${path}`);
            return null;
        }
        console.log(`"${path}" is a directory!`)
        const attrs = {
            "mode": MODE_DIR
        };
        return attrs;
    }
    catch(exc) {
        console.log(`STAT Error: ${exc}`);
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
    hostKeys: [fs.readFileSync('keys/host.key')],
    "greeting": "SFTP-GCS demon",
    "banner": "SFTP-GCS demon"
}, function (client) {
    console.log('Client connected!');

    // ctx.username - The identity of the user asking to be authenticated,
    // ctx.method - How is the request being asked to authenticate?
    // - password - ctx.password contains the password.
    // - publickey - ctx.key, ctx.signature, ctx.blob
    //
    // We must either call ctx.reject() or ctx.accept()
    //
    client.on('authentication', function (ctx) {
        console.log(`authentication: method=${ctx.method}`);
        //var user = Buffer.from(ctx.username);
        switch (ctx.method) {
            case 'none':
                if (allowedUser.length !== 0) {
                    console.log(`We have at least a user to match`);
                    return ctx.reject();
                }
                if (allowedPassword.length !== 0) {
                    console.log(`We have at least a password to match`);
                    return ctx.reject();
                }
                if (allowedPubKey !== null) {
                    console.log(`We want a public key exchange`);
                    return ctx.reject();
                }
                return ctx.accept(); // No userid and no password, come on in!

            case 'password':
                // If a username was supplied when the sftp-gcs app was started and the supplied sftp client
                // username does not match then we can't authenticate.
                if (allowedUser.length > 0 && allowedUser !== ctx.username) {
                    console.log(`usernames did not match`);
                    return ctx.reject();
                }

                // If a password was supplied with the sftp-gcs app was started and the supplied sftp client
                // password does not match then we can't authenticate.
                if (allowedPassword.length > 0 && allowedPassword !== ctx.password) {
                    console.log(`password did not match`)
                    return ctx.reject();
                }

                return ctx.accept();

            case 'publickey':
/**
 *  var allowedPubKey = utils.parseKey(fs.readFileSync('foo.pub'));
 * 
 *  var allowedPubSSHKey = allowedPubKey.getPublicSSH();
 *  if (ctx.key.algo !== allowedPubKey.type
 *    || ctx.key.data.length !== allowedPubSSHKey.length
 *    || !crypto.timingSafeEqual(ctx.key.data, allowedPubSSHKey)
 *    || (ctx.signature && allowedPubKey.verify(ctx.blob, ctx.signature) !== true)) {
 *    return ctx.reject();
 *  }
 */
                console.log(`key: ${util.inspect(ctx.key)}, signature: ${util.inspect(ctx.signature)}`);
                // We have not yet implemented publickey authentication so reject the caller.
                if (allowedPubKey === null) {
                    console.log(`No PubKey`);
                    return ctx.reject();
                }
                var allowedPubSSHKey = allowedPubKey.getPublicSSH();
                if (ctx.key.algo !== allowedPubKey.type
                   || ctx.key.data.length !== allowedPubSSHKey.length
                   || !crypto.timingSafeEqual(ctx.key.data, allowedPubSSHKey)
                   || (ctx.signature && allowedPubKey.verify(ctx.blob, ctx.signature) !== true)) {
                    console.log(`Rejected login`);
                    return ctx.reject();
                }
                console.log(`SSH key allowed login`);
                return ctx.accept();

            default:
                return ctx.reject();
        }

        ctx.accept();
    }); // End on authentication

    client.on('ready', function () {
        console.log('Client authenticated!');

        client.on('session', function (accept, reject) {
            const session = accept();
            session.on('sftp', function (accept, reject) {
                console.log('Client SFTP session');
                const openFiles = new Map(); // The map of open files.

                // Get the file record (the open file) from the set of open files based on the value contained in the
                // handle buffer.  This function either returns a fileRecord object or null if no corresponding file 
                // record object can be found.
                function getFileRecord(handleBuffer) {
                    // Validate that the handle buffer is the right size for a 32bit BE integer.
                    if (handleBuffer.length !== 4) {
                        console.log("ERROR: Buffer wrong size for 32bit BE integer");
                        return null;
                    }

                    const handle = handleBuffer.readUInt32BE(0); // Get the handle of the file from the SFTP client.
                    
                    if (!openFiles.has(handle)) {
                        console.log(`Unable to find file with handle ${handle}`);
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
                    console.log(`OPEN<${reqId}>: filename: "${filename}", flags: ${SFTPStream.flagsToString(flags)}`)

                    filename = normalizePath(filename);
                    
                    const handle = handleCount;
                    handleCount = handleCount + 1;
                    const fileRecord = {
                        "handle": handle
                    };
                    
                    if (flags & SFTPStream.OPEN_MODE.WRITE) {
                        console.log('Opening file for WRITE');

                        // We now need to open the GCS file for writing.  It will be written in subsequent WRITE requests.
                        fileRecord.gcsFile = bucket.file(filename);
                        fileRecord.gcsError = false;
                        fileRecord.writeStream = fileRecord.gcsFile.createWriteStream();
                        fileRecord.writeStream.on('error', (err) => {
                            console.log(`Detected an error with the GCS file: ${err}`);
                            fileRecord.gcsError = true;
                        });
                        fileRecord.writeStream.on('close', () => {
                            console.log('GCS stream closed');
                            if (fileRecord.hasOwnProperty('reqid')) {
                                if (fileRecord.gcsError === true) {
                                    sftpStream.status(fileRecord.reqId, STATUS_CODE.FAILURE);
                                } else {
                                    sftpStream.status(fileRecord.reqId, STATUS_CODE.OK);
                                }
                            }
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
                        console.log(`Opening file for READ`);
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
                            console.log(`GCS readStream: Error: ${err}`);
                        });
                        gcsStream.on('end', () => {
                            console.log(`End of GCS stream`);
                            gcsEnd = true;
                        });
                        gcsStream.on('readable', () => {
                            fileRecord.processQueue();
                        });
                        fileRecord.processQueue = function() {
                            if (gcsEnd) {
                                readMap.forEach((entry) => {
                                    readMap.delete(entry.offset);
                                    entry.resolve(null);
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
                        console.log(`Open mode not supported`);
                        sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        return;
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

                    console.log(`WRITE<${reqId}>: handle: ${fileRecord.handle}, offset ${offset}: data.length=${data.length}`);

                    if (fileRecord.gcsError === true) {
                        console.log(`Returning failure in WRITE because of gcsError`);
                        sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        return;
                    }

                    fileRecord.writeStream.write(data, () => {
                        sftpStream.status(reqId, STATUS_CODE.OK);
                    });                    
                }); // End handle WRITE


                sftpStream.on('READ', async function(reqId, handleBuffer, offset, requestedLength){
                    // READ(< integer >reqID, < Buffer >handle, < integer >offset, < integer >length)

                    const fileRecord = getFileRecord(handleBuffer);
                    if (fileRecord === null) { return sftpStream.status(reqId, STATUS_CODE.FAILURE); }

                    console.log(`READ<${reqId}>: handle: ${fileRecord.handle}, offset: ${offset}, max length: ${requestedLength}`);

                    // Request GCS data starting at a given offset for a requested length.  This is a promise that will
                    // be eventually fulfilled.  The data returned is either a Buffer or null.  If null, that is the
                    // indication that we have reached the end of file.
                    const data = await fileRecord.getGCSData(offset, requestedLength);
                    if (data === null) {  // If we get null, we have reached the end of file.
                        return sftpStream.status(reqId, STATUS_CODE.EOF);
                    }
                    sftpStream.data(reqId, data); // Return the data requested.
                }); // End handle READ

        
                sftpStream.on('MKDIR', async function(reqId, path, attrs) {
                    // MKDIR(< integer >reqID, < string >path, < ATTRS >attrs)
                    console.log(`MKDIR<${reqId}>: path: "${path}", attrs: ${util.inspect(attrs)}`);
                    path = normalizePath(path);
                    const dirName = path + "/";
                    const [exists] = await bucket.file(dirName).exists();
                    if (exists) {
                        console.log(`something call ${dirName} already exists`);
                        sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        return;
                    }
                    const stream = bucket.file(dirName).createWriteStream();
                    stream.end(()=>{
                        sftpStream.status(reqId, STATUS_CODE.OK);
                    });
                }); // End handle MKDIR


                /**
                 * Handle the SFTP OPENDIR request.
                 * * reqId is the request identifier that is returned in a matching response.
                 * * path is the directory that we are going to list
                 */
                sftpStream.on('OPENDIR', async function(reqId, path) {
                    // OPENDIR(< integer >reqID, < string >path)
                    
                    console.log(`OPENDIR<${reqId}> path: "${path}"`);
                    path = normalizePath(path);
                    // Check that we have a directory to list.
                    if (path !== "") {
                        // Return an error
                        // We have handled the simplest case, now we need to see if a directory exists with this name. Imagine we have been 
                        // asked to open "/dir".  This will have been normalized to "dir".  From a GCS perspective, we now want to determine if there are any files
                        // that begin with the prefix "dir/".  If yes, then the directory exists.
                        const [fileList] = await bucket.getFiles({
                            "directory": path,
                            "delimiter": "/",
                            "autoPaginate": false
                        });
                        if (fileList.length == 0) {
                            console.log(`we found no files/directories with directory: "${path}"`);
                            sftpStream.status(reqId, STATUS_CODE.NO_SUCH_FILE);
                            return;
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

                    console.log(`READDIR<${reqId}>: handle: ${fileRecord.handle}`);

                    // When READDIR is called, it is expected to return some (maybe all) of the files in the directory.
                    // It has two return values ... either one or more directory entries or an end of file marker indicating
                    // that all the directory entries have been sent.  In our GCP mapping, on the first call, we return
                    // all the directory entries and on the second call we return an EOF marker.  This satisfies the contract.
                    // After the first call, we set a flag that indicates that the read of the directory is complete and that
                    // subsequent calls should return EOF.
                    if (fileRecord.readComplete) {
                        sftpStream.status(reqId, STATUS_CODE.EOF);
                        return;
                    }

                    bucket.getFiles({
                        "autoPaginate": false,
                        "delimiter": '/',
                        "directory": fileRecord.path
                    }, (err, fileList, nextQuery, apiResponse) => {
// The responses from a GCS file list are two parts.  One part is files in the current "directory" while the other part is the
// list of directories.  This is of course fake as GCS has no concept of directories.
//
                        const results = [];

                        // Find the largest file size ...  We then determine how many characters this is and then
                        // this becomes the padding for the long entry listing.
                        let largest = 0;
                        //console.log(`${util.inspect(fileList[0])}`);
                        fileList.forEach((file) => {
                            if (Number(file.metadata.size) > largest) {
                                largest = file.metadata.size;
                            }
                        });
                        const padding = String(largest).length;

                        fileList.forEach((file) => {
                            if (file.name.endsWith('/')) {
                                return;
                            }
                            const name = file.name.replace(/.*\//,'');
                            console.log(name);
                            results.push({
                                "filename": name,
                                "longname": fileLongEntry(name, false, file.metadata.size, padding, new Date(file.metadata.timeCreated).toISOString()),
                                "attrs": {
                                    "size": file.metadata.size
                                }
                            });
                        });
                        if (apiResponse.prefixes) {
                            apiResponse.prefixes.forEach((filePrefix) => {
                                let fileName = filePrefix.substring(0, filePrefix.length-1);
                                fileName = fileName.replace(/.*\//,'');
                                results.push({
                                    "filename": fileName,
                                    "longname": fileLongEntry(fileName, true, 0, padding, ""),
                                });
                            });
                        }

                        fileRecord.readComplete = true; // Flag that a subseqent call should return EOF.
                        sftpStream.name(reqId, results);
                    });
                   
                }); // End handle READDIR


                sftpStream.on('LSTAT', async function(reqId, path) {
                    // LSTAT(< integer >reqID, < string >path)
                    // use attrs() to send attributes of the requested file back to the client.
                    console.log(`LSTAT<${reqId}>: path: "${path}"`);
                    commonStat(reqId, path);
                }); // End handle LSTAT
                

                sftpStream.on('STAT', async function(reqId, path) {
                    // STAT(< integer >reqID, < string >path)
                    console.log(`STAT<${reqId}>: path: "${path}"`);
                    commonStat(reqId, path);
                });


                sftpStream.on('FSTAT', async function(reqId, handleBuffer) {
                    // FSTAT(< integer >reqID, < Buffer >handle)
                    const fileRecord = getFileRecord(handleBuffer);
                    if (fileRecord === null) { return sftpStream.status(reqId, STATUS_CODE.FAILURE); }

                    console.log(`FSTAT<${reqId}>: handle: ${fileRecord.handle} => path: "${fileRecord.path}"`);
                    commonStat(reqId, fileRecord.path);
                }); // End handle FSTAT


                sftpStream.on('FSETSTAT', function(reqId, handleBuffer, attrs) {
                    // FSETSTAT(< integer >reqID, < Buffer >handle, < ATTRS >attrs)
                    console.log(`FSETSTAT<${reqId}>`);
                    return sftpStream.status(reqId, STATUS_CODE.OP_UNSUPPORTED);
                }); // End handle FSETSTAT
                

                sftpStream.on('RENAME', async function(reqId, oldPath, newPath) {
                    // RENAME(< integer >reqID, < string >oldPath, < string >newPath)
                    console.log(`RENAME<${reqId}>: oldPath: ${oldPath}, newPath: ${newPath}`);
                    oldPath = normalizePath(oldPath);
                    newPath = normalizePath(newPath);
                    // Map the request to a GCS command to rename a GCS object.
                    try {
                        await bucket.file(oldPath).rename(newPath);
                        return sftpStream.status(reqId, STATUS_CODE.OK);
                    } catch(exc) {
                        console.log(exc);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                }); // End of handle RENAME


                sftpStream.on('REMOVE', async function(reqId, path) {
                    // REMOVE(< integer >reqID, < string >path)
                    console.log(`REMOVE<${reqId}>: path: "${path}"`);
                    path = normalizePath(path);
                    if (path.endsWith('/')) { // Sneaky user trying to remove a directory as though it were a file!
                        console.log(`Can't remove a file ending with "/"`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                    // Map the request to a GCS command to delete/remove a GCS object.
                    try {
                        await bucket.file(path).delete();
                        return sftpStream.status(reqId, STATUS_CODE.OK);
                    }
                    catch(exc) {
                        console.log(`Failed to delete file "${path}"`);
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
                    console.log(`RMDIR<${reqId}>: path: "${path}"`);
                    path = normalizePath(path);
                    // If the path does NOT end in with a '/', then add one
                    if (!path.endsWith('/')) {
                        path = path + "/";
                    }

                    // Let us see if we have files that end in path:
                    const [fileList] = await bucket.getFiles({
                        "autoPaginate": false,
                        "delimiter": "/",
                        "prefix": path,
                        "maxResults": 2
                    });
                    if (fileList.length == 0) {
                        console.log(`No such file/directory: "${path}"`);
                        return sftpStream.status(reqId, STATUS_CODE.NO_SUCH_FILE);
                    }
                    if (fileList.length > 1) {
                        console.log(`Directory not empty: "${path}"`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }

                    const [deleteResponse] = await bucket.file(path).delete();
                    return sftpStream.status(reqId, STATUS_CODE.OK);
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

                    console.log(`CLOSE<${reqId}>: handle: ${fileRecord.handle}`);
                    
                    // Close the GCS file stream by calling end().  We save the SFTP request id in the fileRecord.  Notice
                    // that we don't flag the status of this stream request.  Instead, we assume that the call to end will result
                    // in a call to close() which will close the stream and THAT will send back the stream response.
                    
                    openFiles.delete(fileRecord.handle);

                    if (fileRecord.writeStream) {
                        console.log(`Closing GCS write stream`);
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
                    console.log(`REALPATH<${reqId}>: path: "${path}"`);
                    path = PATH.normalize(path);
                    console.log(`Returning "${path}"`);
                    sftpStream.name(reqId, [{ filename: path }]);
                }); // End handle REALPATH
            }); // End on sftp
        }); // End on session
    }); // End on ready
    
    client.on('end', function () {
        console.log('Client disconnected');
    });
    client.on('error', (err) => {
        console.log(`ERROR(client.on): ${util.inspect(err)}`);
    });
}).listen(SERVER_PORT, '0.0.0.0', function () {
    console.log("****************************************");
    console.log("*** Google Cloud Storage SFTP Server ***");
    console.log("****************************************");
    console.log(`Using bucket: gs://${BUCKET_NAME}`);
    console.log('Listening on port ' + this.address().port);
    console.log(`Username: ${allowedUser === ''?'Not set':allowedUser}`);
    console.log(`Password: ${allowedPassword === ''?'Not set':'********'}`);
    console.log(`Public key file: ${publicKeyFile===''?'Not set':publicKeyFile}`);
    console.log(`Service account key file: ${serviceAccountKeyFile===''?'Not set':serviceAccountKeyFile}`);
}).on('error', (err) => {
    // Capture any networking exception.  A common error is that we are asking the sftp-gcs server
    // to listen on a port that is already in use.  Check for that error and call it out specifically.
    console.log(`Error with networking ${util.inspect(err)}`);
    if (err.code === 'EACCES') {
        console.log(`It is likely that an application is already listening on port ${err.port}.`);
    }
});
