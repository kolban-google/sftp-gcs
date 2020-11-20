FROM node:12
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY . .

EXPOSE 22
CMD ["node", "index.js", "--bucket", "kolban-test1", "--port", "9022"]