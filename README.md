# dynamodb-table-entries-gui

A quick and dirty fork of [dynamodb-admin](https://github.com/aaronshaf/dynamodb-admin) that allows you to browse a table in DynamoDB. Use case where I personally needed this was a provisioned DynamoDB that had the table name already provided by the platform (CloudFoundry to be exact) and no access to `ListTables` action on DynamoDB.

This browser allows you to skip `ListTables` alltogether and connect to the table you specified directly through the Web GUI.

The GUI works for both Local and AWS DynamoDB instances. The endpoint url is not required when connecting to AWS.

## Usage

`npm install -g dynamodb-table-entries-browser`

`dynamodb-table-entries-browser --open --port 8001`
```
CLI Options:
 --open / -o - opens server URL in a default browser on start
 --port PORT / -p PORT -  Port to run on (default: 8001)
```

## Screenshots

![Home Page](https://raw.githubusercontent.com/baso53/dynamodb-table-entries-gui/master/github_assets/home_page.jpg)

![Table View](https://raw.githubusercontent.com/baso53/dynamodb-table-entries-gui/master/github_assets/table_view.jpg)
