import { Factory } from 'ember-cli-mirage';

export default Factory.extend({
  isInternal: true,
  tracking: () => ({
    trackers: {
      piwik: {
        piwikSiteId: 1337,
        piwikUrl: '//mock-tracking-url.pwn'
      }
    },
    isEnabled: true
  }),
  status: 'ok',
  appVersion: '0.3.432',
  shouldShowDatasetLineage: true,
  shouldShowDatasetHealth: true,
  suggestionConfidenceThreshold: 50,
  wikiLinks() {
    return {
      gdprPii: 'http://go/gdpr-pii',
      tmsSchema: 'http://go/tms-schema',
      gdprTaxonomy: 'http://go/gdpr-taxonomy#MetadataTaxonomyforDataSets-DatasetLevelTags',
      staleSearchIndex: 'http://go/metadata/faq#MetadataAnnotationFAQs-stale-index',
      dht: 'http://go/dht',
      purgePolicies: 'http://go/gdpr/deletions/purgePolicies',
      jitAcl: 'http://go/jitaclfaq',
      metadataCustomRegex: 'http://go/metadata-custom-regex',
      exportPolicy: 'http://go/metadata/data-export',
      metadataHealth: 'http://go/metadata/health',
      purgeKey: 'http://go/metadata/faq/purge-key',
      datasetDecommission: 'http://go/datasetdeprecation'
    };
  },
  JitAclAccessWhitelist() {
    return ['hdfs', 'hive', 'presto', 'pinot'];
  },
  jitAclContact: 'ask_jitacl@linkedin.com',
  isStagingBanner: true,
  isLiveDataWarning: false,
  isStaleSearch: true,
  showAdvancedSearch: true,
  showFeatures: true,
  showUmp: true,
  useNewBrowseDataset: false,
  showLineageGraph: true,
  userEntityProps() {
    return {
      aviUrlPrimary:
        'https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web/packages/data-portal/public/assets/images/default_avatar.png',
      aviUrlFallback:
        'https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web/packages/data-portal/public/assets/images/default_avatar.png'
    };
  }
});
