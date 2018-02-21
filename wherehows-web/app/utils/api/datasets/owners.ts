import {
  IOwner,
  IOwnerPostResponse,
  IOwnerResponse,
  IOwnerTypeResponse
} from 'wherehows-web/typings/api/datasets/owners';
import {
  IPartyEntity,
  IPartyEntityResponse,
  IPartyProps,
  IUserEntityMap
} from 'wherehows-web/typings/api/datasets/party-entities';
import { datasetUrlById, datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { getJSON, postJSON } from 'wherehows-web/utils/api/fetcher';
import { getApiRoot, ApiStatus } from 'wherehows-web/utils/api/shared';
import { arrayFilter, arrayMap } from 'wherehows-web/utils/array';

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

/**
 * Returns the dataset owners url by urn
 * @param {string} urn
 * @return {string}
 */
const datasetOwnersUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/owners`;

/**
 * Returns the party entities url
 * @type {string}
 */
const partyEntitiesUrl = `${getApiRoot()}/party/entities`;

/**
 * Returns the owner types url
 * @return {string}
 */
const datasetOwnerTypesUrl = () => `${getApiRoot()}/owner/types`;

/**
 * Requests the list of dataset owners from the GET endpoint, converts the modifiedTime property
 * to a date object
 * @param {number} id the dataset Id
 * @return {Promise<Array<IOwner>>} the current list of dataset owners
 */
const readDatasetOwners = async (id: number): Promise<Array<IOwner>> => {
  const { owners = [], status, msg } = await getJSON<IOwnerResponse>({ url: datasetOwnersUrlById(id) });
  if (status === ApiStatus.OK) {
    return ownersWithModifiedTimeAsDate(owners);
  }

  throw new Error(msg);
};

/**
 * Modifies an owner object by applying the modified date property as a Date object
 * @param {IOwner} owner
 * @return {IOwner}
 */
const ownerWithModifiedTimeAsDate = (owner: IOwner): IOwner => ({
  ...owner,
  modifiedTime: new Date(<number>owner.modifiedTime)
}); // Api response is always in number format

/**
 * Describes the mapping function returned from the ownerWithoutPropertiesMappingFnFactory,
 * takes an owner and returns an object with a subset of the properties on IOwner
 * @interface IOwnerWithoutPropMappingFn
 */
interface IOwnerWithoutPropsMappingFn {
  (owner: IOwner): Partial<IOwner>;
}

/**
 * Returns a mapping function of type IOwnerWithoutPropsMappingFn
 * @template K key on the IOwner interface
 * @param {Array<K>} [props=[]] list of keys/ attributes to exclude from the IOwner instance
 * @returns {IOwnerWithoutPropsMappingFn} mapping function
 * TODO: abstract into type-safe fleecing /object filter-map function
 */
const ownerWithoutPropertiesMappingFnFactory = <K extends keyof IOwner>(
  props: Array<K> = []
): IOwnerWithoutPropsMappingFn => (owner: IOwner): Partial<IOwner> => {
  const partialOwner = { ...owner };

  return props.reduce((owner, prop) => {
    delete owner[prop];
    return owner;
  }, partialOwner);
};

/**
 * Modifies a list of owners with a modified date property as a Date object
 * @type {(array: Array<IOwner>) => Array<IOwner>}
 */
const ownersWithModifiedTimeAsDate = arrayMap(ownerWithModifiedTimeAsDate);

/**
 * Reads the owners for dataset by urn
 * @param {string} urn
 * @return {Promise<Array<IOwner>>}
 */
const readDatasetOwnersByUrn = async (urn: string): Promise<Array<IOwner>> => {
  const { owners = [] } = await getJSON<Pick<IOwnerResponse, 'owners'>>({ url: datasetOwnersUrlByUrn(urn) });
  return ownersWithModifiedTimeAsDate(owners);
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
 * Updates the owners on a dataset by urn
 * @param {string} urn
 * @param {string} csrfToken
 * @param {Array<IOwner>} updatedOwners
 * @return {Promise<void>}
 */
const updateDatasetOwnersByUrn = (urn: string, csrfToken: string = '', updatedOwners: Array<IOwner>): Promise<void> => {
  const ownersWithoutModifiedTime = arrayMap(ownerWithoutPropertiesMappingFnFactory(['modifiedTime']));

  return postJSON<void>({
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
const readDatasetOwnerTypes = async (): Promise<Array<OwnerType>> => {
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
const readDatasetOwnerTypesWithoutConsumer = async () => arrayFilter(isNotAConsumer)(await readDatasetOwnerTypes());

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
  readDatasetOwnersByUrn,
  updateDatasetOwnersByUrn,
  readDatasetOwnerTypesWithoutConsumer,
  readPartyEntities,
  readPartyEntitiesMap,
  getUserEntities,
  updateDatasetOwners,
  OwnerIdType,
  OwnerType,
  OwnerUrnNamespace,
  OwnerSource
};
