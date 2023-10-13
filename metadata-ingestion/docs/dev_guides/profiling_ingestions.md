import FeatureAvailability from '@site/src/components/FeatureAvailability';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Profiling ingestions

<FeatureAvailability/>

**🤝 Version compatibility**
> Open Source DataHub: **0.11.1** | Acryl: **0.2.12**

This page documents how to perform memory profiles of ingestion runs. 
It is useful when trying to size the amount of resources necessary to ingest some source or when developing new features or sources.

## How to use
Install the `debug` plugin for DataHub's CLI wherever the ingestion runs:

```bash
pip install 'acryl-datahub[debug]'
```

This will install [memray](https://github.com/bloomberg/memray) in your python environment.

Add a flag to your ingestion recipe to generate a memray memory dump of your ingestion:
```yaml
source:
  ...

sink:
  ...

flags:
  generate_memory_profiles: "<path to folder where dumps will be written to>"
```

Once the ingestion run starts a binary file will be created and appended to during the execution of the ingestion. 

These files follow the pattern `file-<ingestion-run-urn>.bin` for a unique identification.
Once the ingestion has finished you can use `memray` to analyze the memory dump in a flamegraph view using:

```$ memray flamegraph file-None-file-2023_09_18-21_38_43.bin```

This will generate an interactive HTML file for analysis:

<p align="center">
    <img width="70%" src="https://github.com/datahub-project/static-assets/blob/main/imgs/metadata-ingestion/memray-example.png?raw=true"/>
</p>


`memray` has an extensive set of features for memory investigation. Take a look at their [documentation](https://bloomberg.github.io/memray/overview.html) to see the full feature set.


## Questions

If you've got any questions on configuring profiling, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
