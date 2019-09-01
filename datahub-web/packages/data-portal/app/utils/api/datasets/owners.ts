import { IOwner, IOwnerResponse, IOwnerTypeResponse } from 'wherehows-web/typings/api/datasets/owners';
import {
  IPartyEntity,
  IPartyEntityResponse,
  IPartyProps,
  IUserEntityMap
} from 'wherehows-web/typings/api/datasets/party-entities';
import { isNotFoundApiError } from 'wherehows-web/utils/api';
import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import { getApiRoot } from 'wherehows-web/utils/api/shared';
import { ApiStatus } from '@datahub/utils/api/shared';
import { arrayFilter, arrayMap } from 'wherehows-web/utils/array';
import { omit } from 'wherehows-web/utils/object';

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
 * Returns the party entities url
 * @type {string}
 */
export const partyEntitiesUrl = `${getApiRoot()}/party/entities`;

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
 * @return {Promise<void>}
 */
export const updateDatasetOwnersByUrn = (
  urn: string,
  csrfToken: string = '',
  updatedOwners: Array<IOwner>
): Promise<{}> => {
  const ownersWithoutModifiedTime = arrayMap((owner: IOwner) => omit(owner, ['modifiedTime']));

  return postJSON<{}>({
    url: datasetOwnersUrlByUrn(urn),
    headers: { 'csrf-token': csrfToken },
    data: {
      csrfToken,
      owners: ownersWithoutModifiedTime(updatedOwners) // strips the modified time from each owner, remote is source of truth
    }
  });
};

/**
 * Reads the owner types list on a dataset
 * @return {Promise<Array<OwnerType>>}
 */
export const readDatasetOwnerTypes = async (): Promise<Array<OwnerType>> => {
  const url = datasetOwnerTypesUrl();
  const { ownerTypes = [] } = await getJSON<IOwnerTypeResponse>({ url });
  return ownerTypes.sort((a: string, b: string) => a.localeCompare(b));
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

/**
 * Requests party entities and if the response status is OK, resolves with an array of entities
 * @return {Promise<Array<IPartyEntity>>}
 */
export const readPartyEntities = async (): Promise<Array<IPartyEntity>> => {
  const { status, userEntities = [], msg } = await getJSON<IPartyEntityResponse>({ url: partyEntitiesUrl });
  return status === ApiStatus.OK ? userEntities : Promise.reject(msg);
};

/**
 * Transforms a list of party entities into a map of entity label to displayName value
 * @param {Array<IPartyEntity>} partyEntities
 * @return {IUserEntityMap}
 */
export const readPartyEntitiesMap = (partyEntities: Array<IPartyEntity>): IUserEntityMap =>
  partyEntities.reduce(
    (map: { [label: string]: string }, { label, displayName }: IPartyEntity) => ((map[label] = displayName), map),
    {}
  );

/**
 * IIFE prepares the environment scope and returns a closure function that ensures that
 * there is ever only one inflight request for userEntities.
 * Resolves all subsequent calls with the result for the initial invocation.
 * userEntitiesSource property is also lazy evaluated and cached for app lifetime.
 * @type {() => Promise<IPartyProps>}
 */
export const getUserEntities: () => Promise<IPartyProps> = (() => {
  /**
   * Memoized reference to the resolved value of a previous invocation to curried function in getUserEntities
   * @type {{result: IPartyProps | null}}
   */
  const cache: { result: IPartyProps | null; userEntitiesSource: Array<Extract<keyof IUserEntityMap, string>> } = {
    result: null,
    userEntitiesSource: []
  };
  let inflightRequest: Promise<Array<IPartyEntity>>;

  /**
   * Invokes the requestor for party entities, and adds perf optimizations listed above
   * @return {Promise<IPartyProps>}
   */
  return async (): Promise<IPartyProps> => {
    // If a previous request has already resolved, return the cached value
    if (cache.result) {
      return cache.result;
    }
    // If we don't already have a previous api request for party entities,
    // assign a new one to free variable
    if (!inflightRequest) {
      inflightRequest = readPartyEntities();
    }

    const userEntities: Array<IPartyEntity> = await inflightRequest;

    return (cache.result = {
      userEntities,
      userEntitiesMaps: readPartyEntitiesMap(userEntities),
      // userEntitiesSource is not usually needed immediately
      // hence using a getter for lazy evaluation
      get userEntitiesSource() {
        const userEntitiesSource = cache.userEntitiesSource;
        if (userEntitiesSource.length) {
          return userEntitiesSource;
        }

        return (cache.userEntitiesSource = Object.keys(this.userEntitiesMaps));
      }
    });
  };
})();
