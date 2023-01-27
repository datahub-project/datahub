<FeatureAvailability saasOnly />

# Tag Sync

The Tag Sync (or Tag Propagation) Action allows you to propagate tags from your assets into downstream entities. e.g. You can apply a tag (like `critical`) on a dataset and have it propagate down to all the downstream datasets.

### Tutorial

[Here](https://www.loom.com/embed/e73988c1175e4255ac1a84447e248d18) is a quick video showing application of a tag `dev:critical` to a dataset and seeing it propagate downstream to downstream datasets.

<div style={{ position: "relative", paddingBottom: "56.25%", height: 0 }}>
  <iframe
    src="https://www.loom.com/embed/e73988c1175e4255ac1a84447e248d18"
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

### Configurability

You can control which tags should be propagated downstream using a prefix system. E.g. You can specify that only tags that start with `tier:` should be propagated downstream.

### Caveats

* Configuration changes are not automated through the UI and currently require a manual step from the platform operators for your instance
* Tag Propagation is currently only supported for downstream datasets. Tags will not propagate to downstream dashboards or charts. Let us know if this is an important feature for you.
* Tag Sync is currently only "additive". Removing the tag from the upstream dataset will not propagate the removal down to downstream datasets. Let us know if this is an important feature for you.
