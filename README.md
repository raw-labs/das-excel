# DAS Excel
[![License](https://img.shields.io/:license-BSL%201.1-blue.svg)](/licenses/BSL.txt)

[Data Access Service](https://github.com/raw-labs/protocol-das) for Excel.

## Options

| Name                    | Description                                                                                                 | Default | Required |
|-------------------------|-------------------------------------------------------------------------------------------------------------|---------|----------|
| `nr_tables`             | The number of Excel tables in the file to expose                                                            |         | Yes      |
| `filename`              | The path to the Excel file to expose                                                                        |         | Yes      |
| `table0_name`           | The name for the first table                                                                                |         | Yes      |
| `table0_sheet`          | The sheet name where the first table is located                                                             |         | Yes      |
| `table0_region`         | The region where the first table is defined (e.g. `"A1:D100"`)                                              |         | Yes      |
| `table0_header_rows`    | Number of top rows **within the region** that are treated as multi-line headers (0 = none, 1 = single-line) | `0`     | No       |
| `table0_header_joiner`  | The string used to join multiple header lines if `table0_header_rows` > 1                                   | `-`     | No       |
| `table1_name`           | The name for the second table                                                                               |         | Yes      |
| `table1_sheet`          | The sheet name where the second table is located                                                            |         | Yes      |
| `table1_region`         | The region where the second table is defined                                                                |         | Yes      |
| `table1_header_rows`    | Same as above, for the second table                                                                         | `0`     | No       |
| `table1_header_joiner`  | Same as above, for the second table                                                                         | `-`     | No       |
| `...`                   | ... (add more settings for any additional tables in the same pattern) ...                                   |         |          |

### About Headers

- **`tableX_header_rows`**:
    - An integer specifying how many of the top rows in the region should be combined into column headers.
    - If `0`, all columns in that region are auto-named as `A`, `B`, `C`, etc.
    - If `1`, exactly one row of headers is used.
    - If `>1`, multiple rows are merged into one “multi-line” header, joined by `tableX_header_joiner`.

- **`tableX_header_joiner`**:
    - A string that defines how multiple lines of headers are concatenated.
    - Defaults to `-` (dash).
    - Only meaningful if `tableX_header_rows` > 1.

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
