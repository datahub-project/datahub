Ingesting metadata from Snowflake requires either using the **snowflake-beta** module with just one recipe (recommended) or the two separate modules **snowflake** and **snowflake-usage** (soon to be deprecated) with two separate recipes. 

All three modules are described on this page. 

We encourage you to try out the new **snowflake-beta** plugin as alternative to running both **snowflake** and **snowflake-usage** plugins and share feedback. `snowflake-beta` is much faster than `snowflake` for extracting metadata.

## Snowflake Ingestion through the UI

The following video shows you how to ingest Snowflake metadata through the UI.

<div style={{ position: "relative", paddingBottom: "56.25%", height: 0 }}>
  <iframe
    src="https://www.loom.com/embed/15d0401caa1c4aa483afef1d351760db"
    frameBorder={0}
    webkitallowfullscreen=""
    mozallowfullscreen=""
    allowFullScreen=""
    style={{
      position: "absolute",
      top: 0,
      left: 0,
      width: "100%",
      height: "100%"
    }}
  />
</div>


Read on if you are interested in ingesting Snowflake metadata using the **datahub** cli, or want to learn about all the configuration parameters that are supported by the connectors.