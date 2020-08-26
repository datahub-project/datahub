import { HandlerFunction, Schema, Request, MirageRecord } from 'ember-cli-mirage';
import { IOwnerResponse } from '@datahub/data-models/types/entity/dataset/ownership';

export const testDatasetOwnershipUrn = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,kebab-db-name,CORP)';

/**
 * For the supplied urn, find or populate the DB with a reference to the IOwnerResponse
 * @param {Schema} schema the Mirage schema
 * @param {Request} { params: { urn } } parameters supplied with the user request
 */
export const getDatasetOwnership: HandlerFunction = (
  schema: Schema,
  { params: { urn } }: Request
): MirageRecord<IOwnerResponse> => schema.db.datasetOwnerships.firstOrCreate({ urn: String(urn) });
