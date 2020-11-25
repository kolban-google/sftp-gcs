FROM node:12
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY . .

EXPOSE 9022
CMD ["node", "sftp-gcs.js", "--bucket", "kolban-test1", "--port", "9022"]