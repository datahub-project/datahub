import { IDatasetRetention } from 'wherehows-web/typings/api/datasets/retention';

/**
 * 'News' a IDatasetRetention instance with the provided dataset urn
 * @param {string} urn
 * @returns {IDatasetRetention}
 */
const retentionObjectFactory = (urn: string): IDatasetRetention => ({
  datasetUrn: urn,
  purgeNote: '',
  purgeType: ''
});

export { retentionObjectFactory };
