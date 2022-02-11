#! /bin/bash
echo "running end to end tests"

SOURCE_FILE_NAME="fixture/$(uuidgen)-source.json"
startMysql() {
echo "starting mysql container"
docker-compose up -d
}

importData() {
  mysql -h 127.0.0.1 --port 3306 -uroot -pexample < ./fixture/sakila-db/sakila-schema.sql
}

generateSource() {
  touch "$SOURCE_FILE_NAME"
cat << EOF > "$SOURCE_FILE_NAME"
{ "host": "127.0.0.1",
  "database":"sakila",
   "username":"root",
   "password":"example"}
EOF
export SOURCE_CONFIG_FILE="../$SOURCE_FILE_NAME"
}

startMysql
importData
generateSource
go test -test.v ./e2e/
rm "$SOURCE_FILE_NAME"