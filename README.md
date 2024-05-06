# This is impementation of test task for RLD.
## Here is telegram bot that aggregates statistics from sample data.

Input message should be in JSON format like
"{
    "dt_from":"2022-09-01T00:00:00",
    "dt_upto":"2022-12-31T23:59:00",
    "group_type":"month"
}"

## How to run?

1. git clone repo
2. run 'docker compose up -d'
3. run 'docker exec -it <mongodb_container_name> bash'
4. in container console run 'mongorestore --uri mongodb://root:root@127.0.0.1:27017 /app/dump/'
5. Congrats! You can send queries to bot and get responses
