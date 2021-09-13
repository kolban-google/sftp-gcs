# sftp-gcs

SFTP is the ability to transfer files using a protocol built on top of SSH.  Currently, GCP does not have any pre-supplied products to be able to use SFTP to move files to or from Google Cloud Storage (GCS).  There are a number of 3rd party products that are available from GCP marketplace that do offer this ability.

The SFTP protocol is an open standard.  A variety of programming libraries have been developed which implement the protocol.  What this means is that we can use these libraries to implement our own SFTP server application.  We have done just that and used Google Cloud Storage as the back-end storage media for the files.  When an SFTP client connects to our server and it puts or gets files, these are written and read from GCS data.

The application is written in Node.js and has been tested in a variety of runtimes including running as a container.

The current implementation of the solution supports only a single target bucket.

In order for the SFTP application to be able to read and write from GCS, it must have an identity that it can use to authenticate.  The current implementation uses application default credentials.  This means that the application uses the environment configured values.

Arguments:

* `--bucket [BUCKET_NAME]` - The name of the bucket to work against.  This is a **required** parameter.
* `--port [PORT_NUMBER]` - The TCP/IP port number for the server to listen upon.  Defaults to 22.
* `--user [USER_NAME]` - User name for SFTP client access.
* `--password [PASSWORD]` - Password for SFTP client access.
* `--public-key-file [PUBLIC_KEY_FILE]` - File for SSH security for public key connection.
* `--service-account-key-file [KEY_FILE]` - A path to a local file that contains the keys for a Service Account.
* `--debug [DEBUG-LEVEL]` - The logging level at which messages are logged.  The default is `info`.  Set to `debug` for lowest level logging.


The application needs credentials to be able to interact with GCS.  The default is to use the application default credentials for the environment in which the application is running.  These will either be retrieved from the server's metadata (if the application is running on GCP) or from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable if set.  We can use the `--service-account-key-file` to explicitly point to a file local to the application from which service account keys may be retrieved.  If supplied, this will be used in preference to other stories.

When the sftp-gcs server is running we can connect SFTP clients to the server. In order to connect we must provide credentials.  We have choices.

1. `--public-key-file` is supplied, `--user` AND `--password` are NOT supplied: The client must posses a private key for the corresponding public key supplied in `--public-key-file`. 
2. `--user` AND `--public-key-file` are supplied, `--password` is NOT supplied: The client must posses 
   1. the username supplied in `--user` 
   2. a private key for the corresponding public key supplied in `--public-key-file`. 
3. `--user` and `--password` are both supplied: The client can supply a userid/password pair.
4. `--user` and `--password` and `--public-key-file` are NOT supplied: The client need not supply any credentials for access.

See also:

* [Medium article on this project](https://medium.com/google-cloud/sftp-access-to-google-cloud-storage-43ffd6134b0e)
* [sftp command - man(1) - sftp](https://linux.die.net/man/1/sftp)
* [SSH File Transfer Protocol](https://en.wikipedia.org/wiki/SSH_File_Transfer_Protocols)
* [SSH File Transfer Protocol - draft-ietf-secsh-filexfer-02.txt](https://tools.ietf.org/html/draft-ietf-secsh-filexfer-02)
