import StorageArray from 'ember-local-storage/local/array';
import { IStoredEntityAttrs } from '@datahub/shared/types/lists/list';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity/index';

/**
 * Finds a DataModelEntity's stored attributes in a list of IStoredEntityAttrs or vice-versa, using the urn for equality comparison
 * If the urn's match the representations are considered equivalent
 * @template T is assignable to IStoredEntityAttrs or DataModelEntityInstance, not necessarily assignable to U
 * @template U is assignable to IStoredEntityAttrs or DataModelEntityInstance, not necessarily assignable to T
 * @param {(StorageArray<T> | Array<T>)} list the list containing the representations to search through
 */
export const findEntityInList = <
  T extends IStoredEntityAttrs | DataModelEntityInstance,
  U extends IStoredEntityAttrs | DataModelEntityInstance
>(
  list: StorageArray<T> | Array<T>
): ((attributes: U) => U | T | undefined) => ({ urn }: U | T): U | T | undefined => list.findBy('urn', urn);

/**
 * Serializes the interesting attributes from a DataModelEntityInstance that can be used to rehydrate the instance later
 */
export const serializeForStorage = ({ urn, displayName }: DataModelEntityInstance): IStoredEntityAttrs => ({
  urn,
  type: displayName
});
