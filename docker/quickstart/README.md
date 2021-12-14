# Quickstart

These Docker YAML files are used by the [Docker quickstart script](../quickstart.sh) and
the [DataHub CLI quickstart](../../docs/quickstart.md) command.

## Developer Notes
The [DataHub CLI quickstart](../../docs/quickstart.md) command fetches these YAML files from DataHub's GitHub master.
This means, files referenced by earlier releases of DataHub CLI must not be deleted from this directory in order
to preserve backward compatibility.
Otherwise, earlier releases of the DataHub CLI will stop working.

See GitHub issue [linkedin/datahub#3266](https://github.com/linkedin/datahub/issues/3266) for more details.