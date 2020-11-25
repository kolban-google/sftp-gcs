# sftp-gcs

SFTP is the ability to transfer files using a protocol built on top of SSH.  Currently, GCP does not have any pre-supplied products to be able to use SFTP to move files to or from Google Cloud Storage (GCS).  There are a number of 3rd party products that are available from GCP marketplace that do offer this ability.

The SFTP protocol is an open standard.  A variety of programming libraries have been developed which implement the protocol.  What this means is that we can use these libraries to implement our own SFTP server application.  We have done just that and used Google Cloud Storage as the back-end storage media for the files.  When an SFTP client connects to our server and it puts or gets files, these are written and read from GCS data.

The application is written in Node.js and has been tested in a variety of runtimes including running as a container.

The current implementation of the solution supports only a single target bucket.

In order for the SFTP application to be able to read and write from GCS, it must have an identity that it can use to authenticate.  The current implementation uses application default credentials.  This means that the application uses the environment configured values.

Arguments:

* `--bucket [BUCKET_NAME]` - The name of the bucket to work against.  This is **required**.
* `--port [PORT_NUMBER]` - The TCP/IP port number for the server to listen upon.  Defaults to 22.
* `--user [USER_NAME]` - User name for SFTP client access.
* `--password [PASSWORD]` - Password for SFTP client access.
* `--public-key-file [PUBLIC_KEY_FILE]` - File for SSH security for public key connection.
* `--service-account-key-file [KEY_FILE]` - A path to a local file that contains the keys for a Service Account.


The application needs credentials to be able to interact with GCS.  The default is to use the application default credentials for the environment in which the application is running.  These will either be retrieved from the server's metadata (if the application is running on GCP) or from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable if set.  We can use the `--service-account-key-file` to explicitly point to a file local to the application from which service account keys may be retrieved.  If supplied, this will be used in preference to other stories.

When the sftp-gcs server is running we can connect SFTP clients to the server. In order to connect we must provide credentials.  We have choices.

1. The client can posses a private key for the corresponding public key supplied in `--public-key-file`. 
2. The client can supply a userid/password pair.
3. The client need not supply any credentials for access.


See also:

* [sftp command - man(1) - sftp](https://linux.die.net/man/1/sftp)
* [SSH File Transfer Protocol](https://en.wikipedia.org/wiki/SSH_File_Transfer_Protocols)
* [SSH File Transfer Protocol - draft-ietf-secsh-filexfer-02.txt](https://tools.ietf.org/html/draft-ietf-secsh-filexfer-02)
