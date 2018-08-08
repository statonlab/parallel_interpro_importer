# Parallel InterProScan Loader for Tripal

This module provides a `drush` command to load IPS files into CHADO using parallel programming.

## Installation

- Clone this repo into your drupal modules directory:
  - ```bash
    cd sites/all/modules
    git clone https://github.com/statonlab/parallel_interpro_importer.git
    ```
- Install composer dependencies
  - ```bash 
    cd parallel_interpro_importer
    composer install
    ```
- Enable the module
  - `drush en parallel_interpro_importer`


## Usage

The module provides a single drush command `interpro` that has the following options:

|name|type|description|
|----|----|-----------|
|path|`string`|Absolute path to the directory that contains the interpro xml files|
|analysis_id|`int`|The analysis id to associate the annotations with|
|max_jobs|`int`|**Optional.** Number of jobs to run in parallel. Defaults to `5`|
|regexp|`string`|**Optional.** A regular expression to match against the feature. Defaults to `(.*?)`|
|query_type|`string`|**Optional.** The type of feature to match against. Defaults to `mRNA`|

### Examples

To run the loader with 3 threads/processes:

```bash
drush interpro --path=/var/www/html/sites/default/files/ips \
 			   --analysis_id=6 \
 			   --max_jobs=3
```

Specifying a regular expression and query type:

```bash
drush interpro --path=/var/www/html/sites/default/files/ips \
               --analysis_id=6 \
               --regexp="(F.*?):" \
               --query_type="mRNA_contig" \
               --max_jobs=20
```

## Caveats

1. This loader does not use database transactions. Please make sure your data works with
this loader before running it on a production environment.
1. Since this loader splits jobs into multiple threads/processes, you must run the job from within the module's directory
so that it has access to Drupal. Drush's `--root` will not work. Alternatively, you can specify both the `--root` option and add 
the environment variable `DRUPAL_ROOT=/path` to the command as such:
  - `DRUPAL_ROOT=/path drush interpro [OPTIONS] --root=/path`

## License

This module is open source and [licensed under GPLv3](LICENSE).

Copyright 2018 University of Tennessee Knoxville.
