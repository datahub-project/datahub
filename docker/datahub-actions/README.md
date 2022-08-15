# Acryl Actions Container

This container is used to deploy the Actions framework, which can be used to configure actions when the Metadata Graph changes.
This container is maintained by the Acryl Data team. The GitHub repository where the code is hosted is [here](https://github.com/acryldata/trigger-happy). 

## Mounting a Custom Actions Configuration

Actions is driven based on a YAML configuration file. To upload a custom file, you need to perform 2 steps:

1. Mount a custom actions configuration file at container path `/etc/datahub/actions/<your-file-name>.yaml`
2. Configure the actions container to read the custom configuration file by setting the container environment variable `ACTION_FILE_NAME` to the name of the file.
For example, `ACTION_FILE_NAME=<your-file-name>.yaml`. 
