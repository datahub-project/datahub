import { IOwner, IOwnerPostResponse, IOwnerResponse } from 'wherehows-web/typings/api/datasets/owners';
import {
  IPartyEntity,
  IPartyEntityResponse,
  IPartyProps,
  IUserEntityMap
} from 'wherehows-web/typings/api/datasets/party-entities';
import { datasetUrlById } from 'wherehows-web/utils/api/datasets/shared';
import { getJSON, postJSON } from 'wherehows-web/utils/api/fetcher';
import { getApiRoot, ApiStatus } from 'wherehows-web/utils/api/shared';

/**
 * Defines a string enum for valid owner types
 */
enum OwnerIdType {
  User = 'USER',
  Group = 'GROUP'
}

/**
 * Defines the string enum for the OwnerType attribute
 * @type {string}
 */
enum OwnerType {
  Owner = 'Owner',
  Consumer = 'Consumer',
  Delegate = 'Delegate',
  Producer = 'Producer',
  Stakeholder = 'Stakeholder'
}

/**
 * Accepted string values for the namespace of a user
 */
enum OwnerUrnNamespace {
  corpUser = 'urn:li:corpuser',
  groupUser = 'urn:li:corpGroup'
}

enum OwnerSource {
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
 * Constructs the dataset owners url
 * @param {number} id the id of the dataset
 * @return {string} the dataset owners url
 */
const datasetOwnersUrlById = (id: number): string => `${datasetUrlById(id)}/owners`;

const partyEntitiesUrl = `${getApiRoot()}/party/entities`;

/**
 * Requests the list of dataset owners from the GET endpoint, converts the modifiedTime property
 * to a date object
 * @param {number} id the dataset Id
 * @return {Promise<Array<IOwner>>} the current list of dataset owners
 */
const readDatasetOwners = async (id: number): Promise<Array<IOwner>> => {
  const { owners = [], status, msg } = await getJSON<IOwnerResponse>({ url: datasetOwnersUrlById(id) });
  if (status === ApiStatus.OK) {
    return owners.map(owner => ({
      ...owner,
      modifiedTime: new Date(<number>owner.modifiedTime!) // Api response is always in number format
    }));
  }

  throw new Error(msg);
};

/**
 * Persists the updated list of dataset owners
 * @param {number} id the id of the dataset
 * @param {string} csrfToken
 * @param {Array<IOwner>} updatedOwners the updated list of owners for this dataset
 * @return {Promise<IOwnerPostResponse>}
 */
const updateDatasetOwners = async (
  id: number,
  csrfToken: string,
  updatedOwners: Array<IOwner>
): Promise<IOwnerPostResponse> => {
  const { status, msg } = await postJSON<IOwnerPostResponse>({
    url: datasetOwnersUrlById(id),
    headers: { 'csrf-token': csrfToken },
    data: {
      csrfToken,
      owners: updatedOwners
    }
  });

  if ([ApiStatus.OK, ApiStatus.SUCCESS].includes(status)) {
    return { status: ApiStatus.OK };
  }

  throw new Error(msg);
};

/**
 * Requests party entities and if the response status is OK, resolves with an array of entities
 * @return {Promise<Array<IPartyEntity>>}
 */
const readPartyEntities = async (): Promise<Array<IPartyEntity>> => {
  const { status, userEntities = [], msg } = await getJSON<IPartyEntityResponse>({ url: partyEntitiesUrl });
  return status === ApiStatus.OK ? userEntities : Promise.reject(msg);
};

/**
 * IIFE prepares the environment scope and returns a closure function that ensures that
 * there is ever only one inflight request for userEntities.
 * Resolves all subsequent calls with the result for the initial invocation.
 * userEntitiesSource property is also lazy evaluated and cached for app lifetime.
 * @type {() => Promise<IPartyProps>}
 */
const getUserEntities: () => Promise<IPartyProps> = (() => {
  /**
   * Memoized reference to the resolved value of a previous invocation to curried function in getUserEntities
   * @type {{result: IPartyProps | null}}
   */
  const cache: { result: IPartyProps | null; userEntitiesSource: Array<keyof IUserEntityMap> } = {
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

/**
 * Transforms a list of party entities into a map of entity label to displayName value
 * @param {Array<IPartyEntity>} partyEntities
 * @return {IUserEntityMap}
 */
const readPartyEntitiesMap = (partyEntities: Array<IPartyEntity>): IUserEntityMap =>
  partyEntities.reduce(
    (map: { [label: string]: string }, { label, displayName }: IPartyEntity) => ((map[label] = displayName), map),
    {}
  );

export {
  readDatasetOwners,
  readPartyEntities,
  readPartyEntitiesMap,
  getUserEntities,
  updateDatasetOwners,
  OwnerIdType,
  OwnerType,
  OwnerUrnNamespace,
  OwnerSource
};
