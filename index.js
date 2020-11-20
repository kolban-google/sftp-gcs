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

const storage = new Storage();

const OPEN_MODE = ssh2.SFTP_OPEN_MODE;
const STATUS_CODE = ssh2.SFTP_STATUS_CODE;

// Parse the arguments
// * --bucket BUCKET_NAME (required)
// * --port PORT_NUMBER (default: 22)
// * --user USERID
// * --password PASSWORD
//
const argv = require('yargs')
    .usage('$0 --bucket BUCKET_NAME [--port PORT_NUM] [--user USER_NAME] [--password PASSWORD]')
    .options('bucket').describe('bucket','GCS bucket to work with').string('bucket').nargs('bucket', 1).demandOption('bucket', 'Must supply a GCS bucket')
    .options('port').describe('port', 'Port number to listen upon').number('port').nargs('port', 1).default('port', 22, 'Default port is 22')
    .options('user').describe('user', 'Userid for SFTP client').string('user').nargs('user', 1).default('user', '')
    .options('password').describe('password', 'Password for SFTP client').string('password').nargs('password', 1).default('password', '')
    .argv;

// Uncomment the following to log the passed in arguments.
// console.log(`args: ${util.inspect(argv)}`);

const allowedUser = argv.user;
const allowedPassword = argv.password;
const SERVER_PORT = argv.port
const BUCKET_NAME = argv.bucket;
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
                sftpStream.on('OPEN', function (reqid, filename, flags, attrs) {
                    // only allow opening /tmp/foo.txt for writing
                    console.log(`OPEN: ${filename}, flags: ${SFTPStream.flagsToString(flags)}`)
                    /*
                    if (filename !== '/tmp/foo.txt' || !(flags & OPEN_MODE.WRITE))
                        return sftpStream.status(reqid, STATUS_CODE.FAILURE);
                    */
                    // create a fake handle to return to the client, this could easily
                    // be a real file descriptor number for example if actually opening
                    // the file on the disk
                    
                    const handle = handleCount;
                    handleCount = handleCount + 1;
                    const fileRecord = {};
                    openFiles[handle] = fileRecord;
                    
                    console.log('Opening file for write');
                    // We now need to open the GCS file for writing.  It will be written in subsequent WRITE requests.
                    fileRecord.gcsFile = bucket.file(filename);
                    fileRecord.writeStream = fileRecord.gcsFile.createWriteStream();
                    fileRecord.writeStream.on('error', (err) => {
                        console.log(`Detected an error with the GCS file: ${err}`);
                    });

                    const handleBuffer = Buffer.alloc(4);
                    handleBuffer.writeUInt32BE(handle, 0);
                    sftpStream.handle(reqid, handleBuffer);
                    
                }); // End handle OPEN

                //
                // Handle WRITE
                //
                // Called when the client sends a block of data to be written to the
                // file on the SFTP server.
                //
                sftpStream.on('WRITE', function (reqid, handleBuffer, offset, data) {
                    // WRITE(< integer >reqID, < Buffer >handle, < integer >offset, < Buffer >data)
                    const handle = handleBuffer.readUInt32BE(0);
                    if (handleBuffer.length !== 4 || !openFiles[handleBuffer.readUInt32BE(0, true)])
                        return sftpStream.status(reqid, STATUS_CODE.FAILURE);
                    // fake the write
                    const fileRecord = openFiles[handle];
                    fileRecord.writeStream.write(data);
                    var inspected = require('util').inspect(data);
                    console.log('Write to file at offset %d: %s', offset, inspected);
                    sftpStream.status(reqid, STATUS_CODE.OK);
                }); // End handle WRITE

                //
                // Handle CLOSE
                //
                // Called when the client closes the file.  For example at the end of a write.
                //
                sftpStream.on('CLOSE', function (reqid, handleBuffer) {
                    const handle = handleBuffer.readUInt32BE(0, true);
                    if (!openFiles[handle]) {
                        return sftpStream.status(reqid, STATUS_CODE.FAILURE); 
                    }
                    const fileRecord = openFiles[handle];

                    if (handleBuffer.length !== 4 || !openFiles[(fnum = handleBuffer.readUInt32BE(0, true))])
                        return sftpStream.status(reqid, STATUS_CODE.FAILURE);
                    
                    // Close the GCS file stream.
                    fileRecord.writeStream.end();

                    delete openFiles[handle];
                    sftpStream.status(reqid, STATUS_CODE.OK);
                    console.log('Closing file');
                }); // End handle CLOSE

                //
                // Handle REALPATH
                //
                // Called when the client wants to know the full path.
                //
                sftpStream.on('REALPATH', function (reqid, path) {
                    console.log(`REALPATH: ${path}`);
                    sftpStream.name(reqid, [{ filename: "" }]);
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