# File

This plugin pulls metadata from a previously generated file. The file sink
can produce such files, and a number of samples are included in the
[examples/mce_files](../examples/mce_files) directory.

```yml
source:
  type: file
  config:
    filename: ./path/to/mce/file.json
```
