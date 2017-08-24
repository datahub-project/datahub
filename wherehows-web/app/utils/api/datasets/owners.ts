import Ember from 'ember';
import { ApiRoot, ApiStatus } from 'wherehows-web/utils/api/shared';
import { datasetUrlById } from 'wherehows-web/utils/api/datasets/shared';
import {
  IPartyEntity,
  IPartyEntityResponse,
  IPartyProps,
  userEntityMap
} from 'wherehows-web/typings/api/datasets/party-entities';
import { IOwner, IOwnerResponse } from 'wherehows-web/typings/api/datasets/owners';

/**
 * Defines a string enum for valid owner types
 */
export enum OwnerType {
  User = 'USER',
  Group = 'GROUP'
}

const { $: { getJSON } } = Ember;

/**
 * The minimum required number of owners with a confirmed status
 * @type {number}
 */
const minRequiredConfirmed = 2;

/**
 * Constructs the dataset owners url
 * @param {number} id the id of the dataset
 * @return {string} the dataset owners url
 */
const datasetOwnersUrlById = (id: number): string => `${datasetUrlById(id)}/owners`;

const partyEntitiesUrl = `${ApiRoot}/party/entities`;

export const getDatasetOwners = async (id: number): Promise<Array<IOwner>> => {
  const { owners = [], status }: IOwnerResponse = await Promise.resolve(getJSON(datasetOwnersUrlById(id)));
  return status === ApiStatus.OK
    ? owners.map(owner => ({
        ...owner,
        modifiedTime: new Date(owner.modifiedTime || 0)
      }))
    : Promise.reject(status);
};

/**
 * Requests party entities and if the response status is OK, resolves with an array of entities
 * @return {Promise<Array<IPartyEntity>>}
 */
export const getPartyEntities = async (): Promise<Array<IPartyEntity>> => {
  const { status, userEntities = [] }: IPartyEntityResponse = await Promise.resolve(getJSON(partyEntitiesUrl));
  return status === ApiStatus.OK ? userEntities : Promise.reject(status);
};

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
  const cache: { result: IPartyProps | null; userEntitiesSource: Array<keyof userEntityMap> } = {
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
      inflightRequest = getPartyEntities();
    }

    const userEntities: Array<IPartyEntity> = await inflightRequest;

    return (cache.result = {
      userEntities,
      userEntitiesMaps: getPartyEntitiesMap(userEntities),
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
 * @return {Object<string>}
 */
export const getPartyEntitiesMap = (partyEntities: Array<IPartyEntity>): userEntityMap =>
  partyEntities.reduce(
    (map: { [label: string]: string }, { label, displayName }: IPartyEntity) => ((map[label] = displayName), map),
    {}
  );

/**
 * Checks that the required minimum number of confirmed users is met with the type Owner and idType User
 * @param {Array<IOwner>} owners the list of owners to check
 * @return {boolean}
 */
export const isRequiredMinOwnersNotConfirmed = (owners: Array<IOwner> = []): boolean =>
  owners.filter(({ confirmedBy, type, idType }) => confirmedBy && type === 'Owner' && idType === OwnerType.User)
    .length < minRequiredConfirmed;
