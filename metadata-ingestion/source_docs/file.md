# File

This plugin pulls metadata from a previously generated file. The [file sink](../sink_docs/file.md)
can produce such files, and a number of samples are included in the
[examples/mce_files](../examples/mce_files) directory.

```yml
source:
  type: file
  config:
    filename: ./path/to/mce/file.json
```

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
