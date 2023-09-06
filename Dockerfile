FROM node:12
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY . .

EXPOSE $port_var

CMD node sftp-gcs.js --bucket $bucket  --port $port_var --user $username --password $password --service-account-key-file $sa_key_file_path