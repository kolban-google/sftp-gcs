/**
 * Become an SFTP server that uses Google Cloud Storage as the back-end storage
 * system.
 * 
 */
const util = require('util');
const {Storage} = require('@google-cloud/storage');
const fs = require('fs');
const crypto = require('crypto');
const ssh2 = require('ssh2');
const {SFTPStream} = require('ssh2-streams');
const { COPYFILE_EXCL } = require('constants');



const OPEN_MODE = ssh2.SFTP_OPEN_MODE;
const STATUS_CODE = ssh2.SFTP_STATUS_CODE;

const MODE = fs.constants.S_IFREG | fs.constants.S_IRWXU | fs.constants.S_IRWXG | fs.constants.S_IRWXO;

// Parse the arguments
// * --bucket BUCKET_NAME (required)
// * --port PORT_NUMBER (default: 22)
// * --user USERID
// * --password PASSWORD
// * --service-account-key-file KEY_FILE
//
const argv = require('yargs')
    .usage('$0 --bucket BUCKET_NAME [--port PORT_NUM] [--user USER_NAME] [--password PASSWORD]')
    .options('bucket').describe('bucket','GCS bucket to work with').string('bucket').nargs('bucket', 1).demandOption('bucket', 'Must supply a GCS bucket')
    .options('port').describe('port', 'Port number to listen upon').number('port').nargs('port', 1).default('port', 22, 'Default port is 22')
    .options('user').describe('user', 'Userid for SFTP client').string('user').nargs('user', 1).default('user', '')
    .options('password').describe('password', 'Password for SFTP client').string('password').nargs('password', 1).default('password', '')
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
const SERVER_PORT = argv.port
const BUCKET_NAME = argv.bucket;

const storageOptions = {};
if (serviceAccountKeyFile.length > 0) {
    storageOptions.keyFilename = serviceAccountKeyFile;
}
const storage = new Storage(storageOptions); // Get access to the GCS environment
const bucket = storage.bucket(BUCKET_NAME);


new ssh2.Server({
    hostKeys: [fs.readFileSync('keys/host.key')]
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
        var user = Buffer.from(ctx.username);
        switch (ctx.method) {
            case 'none':
                if (allowedUser.length === 0 && allowedPassword.length === 0) {
                    return ctx.accept();
                }
                return ctx.reject();

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
                console.log(`key: ${util.inspect(ctx.key)}, signature: ${ctx.signature}}`);
                // We have not yet implemented publickey authentication so reject the caller.
                return ctx.reject();

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
                const openFiles = {}; // The set of open files.
                let handleCount = 0;
                // `sftpStream` is an `SFTPStream` instance in server mode
                // see: https://github.com/mscdex/ssh2-streams/blob/master/SFTPStream.md
                const sftpStream = accept();

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
                sftpStream.on('OPEN', function (reqId, filename, flags, attrs) {
                    console.log(`OPEN<${reqId}>: filename: ${filename}, flags: ${SFTPStream.flagsToString(flags)}`)
                    
                    const handle = handleCount;
                    handleCount = handleCount + 1;
                    const fileRecord = {};
                    openFiles[handle] = fileRecord;
                    
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
                                    sftpStream.status(fileRecord.reqid, STATUS_CODE.FAILURE);
                                } else {
                                    sftpStream.status(fileRecord.reqid, STATUS_CODE.OK);
                                }
                            }
                        });
                    } else if (flags & SFTPStream.OPEN_MODE.READ) {
                        console.log(`Opening file for READ`);
                        fileRecord.gcsFile = bucket.file(filename);
                        fileRecord.gcsError = false;
                        fileRecord.gcsEnd = false;
                        fileRecord.gcsOffset = 0;
                        fileRecord.readStream = fileRecord.gcsFile.createReadStream();
                        fileRecord.readStream.on('error', (err) => {
                            console.log(`GCS readStream: Error: ${err}`);
                        });
                        /*
                        fileRecord.readStream.on('readable', () => {
                            console.log(`Readable called`);
                            fileRecord.readStream.removeAllListeners('readable');
                            const data = fileRecord.readStream.read();
                            console.log(`data: ${util.inspect(data)}`);
                        });
                        */
                       /*
                        fileRecord.readStream.on('end', () => {
                            console.log('GCS readStream: end');
                            fileRecord.gcsEnd = true;
                        });
                        */
                        /*
                        fileRecord.readStream.on('data', (chunk) => {
                            console.log(`Received ${chunk.length} bytes of data.`);
                        });
                        */
                        //fileRecord.readStream.pause();

                    } else {
                        console.log(`Open mode not supported`);
                        sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        return;
                    }

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
                    console.log(`WRITE<${reqId}> to file at offset ${offset}: ${util.inspect(data)}`);

                    if (handleBuffer.length !== 4) {
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }

                    const handle = handleBuffer.readUInt32BE(0);
                    
                    if (!openFiles[handle]) {
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                    
                    const fileRecord = openFiles[handle];
                    if (fileRecord.gcsError === true) {
                        console.log(`Returning failure in WRITE because of gcsError`);
                        sftpStream.status(reqId, STATUS_CODE.FAILURE);
                        return;
                    }

                    fileRecord.writeStream.write(data);

                    sftpStream.status(reqId, STATUS_CODE.OK);
                }); // End handle WRITE

                sftpStream.on('READ', function(reqId, handleBuffer, offset, length){
                    // READ(< integer >reqID, < Buffer >handle, < integer >offset, < integer >length)
                    console.log(`READ<${reqId}> offset: ${offset}, max length: ${length}`);

                    if (handleBuffer.length !== 4) {
                        console.log("ERROR");
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }

                    const handle = handleBuffer.readUInt32BE(0);
                    
                    if (!openFiles[handle]) {
                        console.log(`Unable to find handle`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }

                    const fileRecord = openFiles[handle];
                    //console.log(`readableLength: ${fileRecord.readStream.readableLength}, readable: ${fileRecord.readStream.readable}, readableFlowing: ${fileRecord.readStream.readableFlowing}`)

                    if (fileRecord.readStream.readableEnded) {
                        console.log(`Readable has ended`)
                        sftpStream.status(reqId, STATUS_CODE.EOF);
                        return;
                    }

                    function onReadable() {
                        console.log(`onReadable fired for ${reqId} called within READ: readableLength: ${fileRecord.readStream.readableLength}`);
                        fileRecord.readStream.removeListener('end', onEnd);
                        fileRecord.readStream.removeListener('readable', onReadable);
                        const data = fileRecord.readStream.read();
                        //console.log(`Data read from gcs: ${data}`);
                        console.log(`data: ${util.inspect(data)}`);
                        if (data !== null) {
                            console.log(`< ${reqId}: Returning data for ${fileRecord.gcsOffset} to ${fileRecord.gcsOffset+ data.length} of size ${data.length}`);
                            fileRecord.gcsOffset += data.length;
                            sftpStream.data(reqId, data);
                        } else {
                            console.log(`Returning EOF`);
                            sftpStream.status(reqId, STATUS_CODE.EOF);
                        }
                    }
                    function onEnd() {
                        console.log(`onEnd fired for ${reqId}`);
                        fileRecord.readStream.removeListener('end', onEnd);
                        fileRecord.readStream.removeListener('readable', onReadable);
                        console.log(`onEnd: Returning EOF`);
                        sftpStream.status(reqId, STATUS_CODE.EOF);
                    }

                    fileRecord.readStream.on('end', onEnd);
                    fileRecord.readStream.on('readable', onReadable);

                    

                    /*
                    const data = fileRecord.readStream.read();
                    console.log(`Data read from gcs: ${data}`);
                    if (data === null) {
                        return;
                    }
                    sftpStream.data(data);
                    */
                }); // End handle READ
                
                sftpStream.on('OPENDIR', function(reqId, path) {
                    // OPENDIR(< integer >reqID, < string >path)
                    console.log(`OPENDIR<${reqId}> path: ${path}`);
                    return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                });

                sftpStream.on('READDIR', function(reqId, handleBuffer) {
                    // READDIR(< integer >reqID, < Buffer >handle)
                    console.log(`READDIR<${reqId}>`);
                    return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                });

                sftpStream.on('LSTAT', async function(reqId, path) {
                    // LSTAT(< integer >reqID, < string >path)
                    // use attrs() to send attributes of the requested file back to the client.
                    console.log(`LSTAT<${reqId}: path: ${path}>`);
                    // Get the details of the GCS object.

                    try {
                        const [metadata] = await bucket.file(path).getMetadata();
                        console.log(`metadata: ${util.inspect(metadata)}`);
                        const attrs = {
                            mode: MODE,
                            uid: 0,
                            gid: 0,
                            size: Number(metadata.size),
                            atime: 1605895676,
                            mtime: 1605895676
                        };
                        sftpStream.attrs(reqId, attrs);
                    }
                    catch(exc) {
                        console.log(`LSTAT: ${exc}`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                    
                    return;
                }); // End handle LSTAT
                
                sftpStream.on('STAT', async function(reqId, path) {
                    // STAT(< integer >reqID, < string >path)
                    console.log(`STAT<${reqId}: path: ${path}>`);
                    try {
                        const [exists] = await bucket.file(path).exists();
                        if (!exists) {
                            return sftpStream.status(reqId, STATUS_CODE.NO_SUCH_FILE);
                        }
                        const [metadata] = await bucket.file(path).getMetadata();
                        console.log(`metadata: ${util.inspect(metadata)}`);
                        const attrs = {
                            mode: MODE,
                            uid: 0,
                            gid: 0,
                            size: Number(metadata.size),
                            atime: 0,
                            mtime: 0
                        };
                        sftpStream.attrs(reqId, attrs);
                    }
                    catch(exc) {
                        console.log(`STAT: ${exc}`);
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                    }
                    
                    return;
                });

                sftpStream.on('FSTAT', function(reqId, handle) {
                    // FSTAT(< integer >reqID, < Buffer >handle)
                    console.log(`FSTAT<${reqId}>`);
                    return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                });

                sftpStream.on('FSETSTAT', function(reqId, handle, attrs) {
                    // FSETSTAT(< integer >reqID, < Buffer >handle, < ATTRS >attrs)
                    console.log(`FSETSTAT<${reqId}>`);
                    return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                });
                
                
                sftpStream.on('REMOVE', function(reqId, path) {
                    // REMOVE(< integer >reqID, < string >path)
                    console.log(`REMOVE<${reqId}>`);
                    return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                });

                sftpStream.on('RMDIR', function(reqId, path) {
                    // RMDIR(< integer >reqID, < string >path)
                    console.log(`RMDIR<${reqId}>`);
                    return sftpStream.status(reqId, STATUS_CODE.FAILURE);
                });
                
                //
                // Handle CLOSE
                //
                // Called when the client closes the file.  For example at the end of a write.
                //
                sftpStream.on('CLOSE', function (reqId, handleBuffer) {
                    console.log(`CLOSE<${reqId}>`);
                    const handle = handleBuffer.readUInt32BE(0);
                    if (!openFiles[handle]) {
                        return sftpStream.status(reqId, STATUS_CODE.FAILURE); 
                    }
                    const fileRecord = openFiles[handle];
                    
                    // Close the GCS file stream by calling end().  We save the SFTP request id in the fileRecord.  Notice
                    // that we don't flag the status of this stream request.  Instead, we assume that the call to end will result
                    // in a call to close() which will close the stream and THAT will send back the stream response.
                    
                    delete openFiles[handle];

                    fileRecord.reqid = reqId;
                    if (fileRecord.writeStream) {
                        fileRecord.writeStream.end();
                        return sftpStream.status(reqId, STATUS_CODE.OK);
                    } else {
                        return sftpStream.status(reqId, STATUS_CODE.OK);
                    }
                    
                    /*
                    if (fileRecord.gcsError === true) {
                        console.log('close error');
                        //sftpStream.status(reqid, STATUS_CODE.FAILURE);
                        return;
                    }
                    */
                    //sftpStream.status(reqid, STATUS_CODE.OK);

                }); // End handle CLOSE

                //
                // Handle REALPATH
                //
                // Called when the client wants to know the full path.
                //
                sftpStream.on('REALPATH', function (reqId, path) {
                    console.log(`REALPATH: ${path}`);
                    sftpStream.name(reqId, [{ filename: "" }]);
                    //sftpStream.status(reqid, STATUS_CODE.OK);
                }); // End handle REALPATH
            }); // End on sftp
        }); // End on session
    }); // End on ready
    
    client.on('end', function () {
        console.log('Client disconnected');
    });
}).listen(SERVER_PORT, '0.0.0.0', function () {
    console.log("****************************************");
    console.log("*** Google Cloud Storage SFTP Server ***");
    console.log("****************************************");
    console.log('Listening on port ' + this.address().port);
    console.log(`Using bucket: gs://${BUCKET_NAME}`);
});