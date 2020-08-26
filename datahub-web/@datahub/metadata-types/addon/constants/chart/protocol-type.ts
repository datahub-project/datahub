/**
 * https://jarvis.corp.linkedin.com/codesearch/result/?name=InchartsChartQuery.pdsc&path=metadata-models%2Fmetadata-models%2Fsrc%2Fmain%2Fpegasus%2Fcom%2Flinkedin%2Fchart&reponame=multiproducts%2Fmetadata-models#InchartsChartQuery
 * The type of protocol for the chart query.
 */
export enum ProtocolType {
  // Use Raptor query language for the chart query.
  'RQL' = 'RQL',
  // Use Prism for the chart query.
  'PRISM' = 'PRISM',
  // Use Rshiny iframe spec for the chart query.
  'RSHINY' = 'RSHINY',
  // Use Vizpack spec for the chart query.
  'VIZPACK' = 'VIZPACK'
}
