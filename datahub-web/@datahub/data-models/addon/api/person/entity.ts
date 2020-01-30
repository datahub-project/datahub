import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';
import { encodeUrn } from '@datahub/utils/validators/urn';
import { ICorpUserInfo } from '@datahub/metadata-types/types/entity/person/person-entity';
import { getJSON, postJSON } from '@datahub/utils/api/fetcher';

/**
 * Constructs the Person url root endpoint
 * @param {ApiVersion} version the version of the api applicable to retrieve the Person
 */
export const personUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/corpusers`;

/**
 * Constructs the url for a person identified by the provided string urn
 * @param {string} urn the urn to use in querying for person entity
 */
export const personUrlByUrn = (urn: string): string => `${personUrlRoot(ApiVersion.v2)}/${encodeUrn(urn)}`;

/**
 * Constructs the person url for the endpoint to update editable info
 * @param {string} urn - the urn identifier for the person entity
 */
const personEditableInfoUrlByUrn = (urn: string): string => `${personUrlByUrn(urn)}/editableInfo`;

/**
 * Queries the person endpoint with the urn provided to retrieve entity information
 * @param {string} urn
 */
export const readPerson = (urn: string): Promise<ICorpUserInfo> => getJSON({ url: personUrlByUrn(urn) });

/**
 * Writes to the person endpoint with the update to a person's editable info
 * @param {string} urn - the unique identifier for the person entity
 * @param {ICorpUserInfo['editableInfo']} data - the data to send to the endpoint to update editable info
 */
export const saveEditablePersonalInfo = (urn: string, data: ICorpUserInfo['editableInfo']): Promise<void> =>
  postJSON({
    data,
    url: personEditableInfoUrlByUrn(urn)
  });
