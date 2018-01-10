export default [
  {
    supportedPurgePolicies: ['AUTO_PURGE', 'MANUAL_PURGE', 'MANUAL_LIMITED_RETENTION', 'PURGE_EXEMPTED'],
    type: 'DISTRIBUTED_KEY_VALUE_STORE',
    name: 'espresso'
  },
  {
    supportedPurgePolicies: [
      'AUTO_PURGE',
      'MANUAL_PURGE',
      'LIMITED_RETENTION',
      'MANUAL_LIMITED_RETENTION',
      'PURGE_EXEMPTED'
    ],
    type: 'DISTRIBUTED_FILE_SYSTEM',
    name: 'hdfs'
  },
  {
    supportedPurgePolicies: [
      'AUTO_PURGE',
      'MANUAL_PURGE',
      'LIMITED_RETENTION',
      'MANUAL_LIMITED_RETENTION',
      'PURGE_EXEMPTED'
    ],
    type: 'DISTRIBUTED_FILE_SYSTEM',
    name: 'hive'
  },
  {
    supportedPurgePolicies: ['LIMITED_RETENTION'],
    type: 'DISTRIBUTED_MESSAGE_BROKER',
    name: 'kafka'
  },
  {
    supportedPurgePolicies: ['MANUAL_PURGE', 'MANUAL_LIMITED_RETENTION', 'PURGE_EXEMPTED'],
    type: 'RELATIONAL_DB',
    name: 'mysql'
  },
  {
    supportedPurgePolicies: ['MANUAL_PURGE', 'MANUAL_LIMITED_RETENTION', 'PURGE_EXEMPTED'],
    type: 'RELATIONAL_DB',
    name: 'oracle'
  },
  {
    supportedPurgePolicies: [
      'AUTO_PURGE',
      'MANUAL_PURGE',
      'LIMITED_RETENTION',
      'MANUAL_LIMITED_RETENTION',
      'PURGE_EXEMPTED'
    ],
    type: 'RELATIONAL_DB',
    name: 'teradata'
  },
  {
    supportedPurgePolicies: ['MANUAL_LIMITED_RETENTION', 'PURGE_EXEMPTED'],
    type: 'DISTRIBUTED_KEY_VALUE_STORE',
    name: 'venice'
  },
  {
    supportedPurgePolicies: ['MANUAL_LIMITED_RETENTION', 'PURGE_EXEMPTED'],
    type: 'DISTRIBUTED_KEY_VALUE_STORE',
    name: 'voldemort'
  },
  {
    supportedPurgePolicies: ['MANUAL_PURGE', 'MANUAL_LIMITED_RETENTION', 'PURGE_EXEMPTED'],
    type: 'DISTRIBUTED_KEY_VALUE_STORE',
    name: 'couchbase'
  },
  {
    supportedPurgePolicies: ['MANUAL_PURGE', 'MANUAL_LIMITED_RETENTION', 'PURGE_EXEMPTED'],
    type: 'DISTRIBUTED_OBJECT_STORE',
    name: 'ambry'
  }
];
