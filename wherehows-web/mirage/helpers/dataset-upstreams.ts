const getDatasetUpstreams = ({ datasetViews }: { datasetViews: any }) =>
  datasetViews.all().models.map((datasetView: any) => ({
    dataset: datasetView,
    actor: 'corpuser:lskywalker',
    type: 'FAKE-TYPE'
  }));

export { getDatasetUpstreams };
