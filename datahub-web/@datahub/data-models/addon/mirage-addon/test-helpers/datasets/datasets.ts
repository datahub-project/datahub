import { Server } from 'ember-cli-mirage';
import { getDatasetUrnParts } from '@datahub/data-models/entity/dataset/utils/urn';
/**
 * Will create a dataset given a urn in mirage
 * @param server
 * @param name
 */
export const createDataset = (
  server: Server,
  urn: string,
  traits: Partial<Com.Linkedin.Dataset.Dataset> = {}
): void => {
  const { platform, prefix, fabric } = getDatasetUrnParts(urn);
  server.create('dataset', { ...traits, platform, name: prefix, origin: fabric });
};
