import { IOwnerResponse, IOwner } from '@datahub/data-models/types/entity/dataset/ownership';
import { getJSON } from '@datahub/utils/api/fetcher';
import { isNotFoundApiError } from '@datahub/utils/api/shared';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';

/**
 * Shared Dataset ownership mid-tier endpoint
 */
export const ownershipEndpoint = 'owners';

/**
 * Modifies an owner object by applying the modified date property as a Date object
 * @param {IOwner} owner an instance of a Dataset Owner
 */
export const ownerWithModifiedTimeAsDate = (owner: IOwner): IOwner => ({
  ...owner,
  modifiedTime: new Date(owner.modifiedTime as number)
});

/**
 * Returns the dataset owners url by urn
 * @param {string} urn the related dataset urn for which ownership is sought
 */
export const datasetOwnersUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/${ownershipEndpoint}`;

/**
 * Reads the owners for dataset by urn
 * @param {string} urn associated dataset urn to request ownership information of
 */
export const readDatasetOwnersByUrn = async (urn: string): Promise<IOwnerResponse> => {
  let owners: Array<IOwner> = [],
    fromUpstream = false,
    datasetUrn = '',
    lastModified = 0,
    actor = '';

  try {
    ({ owners = [], fromUpstream, datasetUrn, actor, lastModified } = await getJSON({
      url: datasetOwnersUrlByUrn(urn)
    }));

    return { owners: owners.map(ownerWithModifiedTimeAsDate), fromUpstream, datasetUrn, actor, lastModified };
  } catch (e) {
    if (isNotFoundApiError(e)) {
      return { owners, fromUpstream, datasetUrn, actor, lastModified };
    }

    throw e;
  }
};
