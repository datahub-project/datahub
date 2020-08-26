import { IOwner, IOwnerResponse, IOwnerTypeResponse } from 'datahub-web/typings/api/datasets/owners';
import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import { getApiRoot, isNotFoundApiError } from '@datahub/utils/api/shared';
import { arrayFilter, arrayMap } from '@datahub/utils/array/index';
import { omit } from 'datahub-web/utils/object';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';

/**
 * Defines a string enum for valid owner types
 */
export enum OwnerIdType {
  User = 'USER',
  Group = 'GROUP'
}

/**
 * Defines the string enum for the OwnerType attribute
 * @type {string}
 */
export enum OwnerType {
  Owner = 'DataOwner',
  Consumer = 'Consumer',
  Delegate = 'Delegate',
  Producer = 'Producer',
  Stakeholder = 'Stakeholder'
}

/**
 * Accepted string values for the namespace of a user
 */
export enum OwnerUrnNamespace {
  corpUser = 'urn:li:corpuser',
  groupUser = 'urn:li:corpGroup',
  multiProduct = 'urn:li:multiProduct'
}

export enum OwnerSource {
  Scm = 'SCM',
  Nuage = 'NUAGE',
  Sos = 'SOS',
  Db = 'DB',
  Audit = 'AUDIT',
  Jira = 'JIRA',
  RB = 'RB',
  Ui = 'UI',
  Fs = 'FS',
  Other = 'OTHER'
}

/**
 * Returns the dataset owners url by urn
 * @param {string} urn
 * @return {string}
 */
export const datasetOwnersUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/owners`;

/**
 * Returns the url to fetch the suggested dataset owners by urn
 * @param urn
 */
export const datasetSuggestedOwnersUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/owners/suggestion`;

/**
 * Returns the owner types url
 * @return {string}
 */
export const datasetOwnerTypesUrl = (): string => `${getApiRoot()}/owner/types`;

/**
 * Modifies an owner object by applying the modified date property as a Date object
 * @param {IOwner} owner
 * @return {IOwner}
 */
export const ownerWithModifiedTimeAsDate = (owner: IOwner): IOwner => ({
  ...owner,
  modifiedTime: new Date(owner.modifiedTime as number)
}); // Api response is always in number format

/**
 * Modifies a list of owners with a modified date property as a Date object
 * @type {(array: Array<IOwner>) => Array<IOwner>}
 */
export const ownersWithModifiedTimeAsDate = arrayMap(ownerWithModifiedTimeAsDate);

/**
 * Reads the owners for dataset by urn
 * @param {string} urn
 * @return {Promise<Array<IOwner>>}
 */
export const readDatasetOwnersByUrn = async (urn: string): Promise<IOwnerResponse> => {
  let owners: Array<IOwner> = [],
    fromUpstream = false,
    datasetUrn = '',
    lastModified = 0,
    actor = '';

  try {
    ({ owners = [], fromUpstream, datasetUrn, actor, lastModified } = await getJSON<IOwnerResponse>({
      url: datasetOwnersUrlByUrn(urn)
    }));
    return { owners: ownersWithModifiedTimeAsDate(owners), fromUpstream, datasetUrn, actor, lastModified };
  } catch (e) {
    if (isNotFoundApiError(e)) {
      return { owners, fromUpstream, datasetUrn, actor, lastModified };
    } else {
      throw e;
    }
  }
};

/**
 * For the specific dataset, fetches the system suggested owners for that dataset
 * @param urn - unique identifier for the dataset
 * @return {Promise<Array<IOwner>>}
 */
export const readDatasetSuggestedOwnersByUrn = async (urn: string): Promise<IOwnerResponse> => {
  let owners: Array<IOwner> = [],
    fromUpstream = false,
    datasetUrn = '',
    lastModified = 0,
    actor = '';

  try {
    ({ owners = [], fromUpstream, datasetUrn, actor, lastModified } = await getJSON<IOwnerResponse>({
      url: datasetSuggestedOwnersUrlByUrn(urn)
    }));
    return { owners: ownersWithModifiedTimeAsDate(owners), fromUpstream, datasetUrn, actor, lastModified };
  } catch (e) {
    if (isNotFoundApiError(e)) {
      return { owners, fromUpstream, datasetUrn, actor, lastModified };
    } else {
      throw e;
    }
  }
};

/**
 * Updates the owners on a dataset by urn
 * @param {string} urn
 * @param {string} csrfToken
 * @param {Array<IOwner>} updatedOwners
 */
export const updateDatasetOwnersByUrn = (urn: string, updatedOwners: Array<IOwner>): Promise<{}> => {
  const ownersWithoutModifiedTime = arrayMap(
    (owner: IOwner): Exclude<IOwner, 'modifiedTime'> => omit(owner, ['modifiedTime'])
  );

  return postJSON<{}>({
    url: datasetOwnersUrlByUrn(urn),
    data: {
      owners: ownersWithoutModifiedTime(updatedOwners) // strips the modified time from each owner, remote is source of truth
    }
  });
};

/**
 * Reads the owner types list on a dataset
 */
export const readDatasetOwnerTypes = async (): Promise<Array<OwnerType>> => {
  const url = datasetOwnerTypesUrl();
  const { ownerTypes = [] } = await getJSON<IOwnerTypeResponse>({ url });

  return ownerTypes.sort((a: string, b: string): number => a.localeCompare(b));
};

/**
 * Determines if an owner type supplied is not of type consumer
 * @param {OwnerType} ownerType
 * @return {boolean}
 */
const isNotAConsumer = (ownerType: OwnerType): boolean => ownerType !== OwnerType.Consumer;

/**
 * Reads the dataset owner types and filters out the OwnerType.Consumer type from the list
 * @return {Promise<Array<OwnerType>>}
 */
export const readDatasetOwnerTypesWithoutConsumer = async (): Promise<Array<OwnerType>> =>
  arrayFilter(isNotAConsumer)(await readDatasetOwnerTypes());
