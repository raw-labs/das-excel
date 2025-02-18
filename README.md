# DAS Excel
[![License](https://img.shields.io/:license-BSL%201.1-blue.svg)](/licenses/BSL.txt)

[Data Access Service](https://github.com/raw-labs/protocol-das) for Excel.

## Options

| Name             | Description                                                                          | Default | Required |
|------------------|--------------------------------------------------------------------------------------|---------|----------|
| `nr_tables`      | The number of Excel tables in the file to Expose                                     |         | Yes      |
| `filename`       | The path to the Excel file to expose                                                 |         | Yes      |
| `table0_name`    | The name for the first table                                                         |         | Yes      |
| `table0_sheet`   | The sheet name where the first table is located                                      |         | Yes      |
| `table0_region`  | The region where the first table is defined, e.g."A1:D100"                           |         | Yes      |
| `table0_headers` | true/false indicating whether the first row of the first table contains the header   |         | Yes      |
| `table1_name`    | The name for the second table                                                        |         | Yes      |
| `table1_sheet`   | The sheet name where the second table is located                                     |         | Yes      |
| `table1_region`  | The region where the second table is defined, e.g."A1:D100"                          |         | Yes      |
| `table1_headers` | true/false indicating whether the first  row of the second table contains the header |         | Yes      |
| `...`            | ... (add more settings for the remainder of the tables) ...                          |         | Yes      |

## How to use

First you need to build the project:
```bash
$ sbt "project docker" "docker:publishLocal"
```

This will create a docker image with the name `das-excel`.

Then you can run the image with the following command:
```bash
$ docker run -p 50051:50051 <image_id>
```
... where `<image_id>` is the id of the image created in the previous step.
This will start the server, typically on port 50051.

You can find the image id by looking at the sbt output or by running:
```bash
$ docker images
```
