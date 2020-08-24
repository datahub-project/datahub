import { IInstitutionalMemory } from '@datahub/metadata-types/types/aspects/institutional-memory';
import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';

/**
 * Constructs the url for institutional memory
 * @param {string} urn - the urn for the entity
 */
const institutionalMemoryByUrn = (urn: string, entityType: string): string =>
  `${getApiRoot(ApiVersion.v2)}/${entityType}s/${urn}/institutionalmemory`;

/**
 * Fetches the list of wiki-type url links to documents related to an entity
 * @param {string} urn - urn for the entity
 */
export const readInstitutionalMemory = (
  urn: string,
  entityType: string
): Promise<{ elements: Array<IInstitutionalMemory> }> => getJSON({ url: institutionalMemoryByUrn(urn, entityType) });

/**
 * Returns a view of the full list of wiki-type url links to documents related to an entity. This should be how the
 * list appears after any changes by the user (add or delete)
 * @param {string} urn - urn for the entity
 * @param {Array<IInstitutionalMemory>} wikiLinks - related links snapshot
 */
export const writeInstitutionalMemory = (
  urn: string,
  entityType: string,
  wikiLinks: Array<IInstitutionalMemory>
): Promise<void> =>
  postJSON({
    url: institutionalMemoryByUrn(urn, entityType),
    data: { elements: wikiLinks }
  });
