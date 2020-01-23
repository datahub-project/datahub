import { IInstitutionalMemory } from '@datahub/data-models/types/entity/common/wiki/institutional-memory';
import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';

/**
 * Constructs the url for a datasets institutional memory
 * @param {string} urn - the urn for the dataset
 */
const datasetInstitutionalMemoryByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/institutionalmemory`;

/**
 * Fetches the list of wiki-type url links to documents related to a dataset
 * @param {string} urn - urn for the dataset
 */
export const readDatasetInstitutionalMemory = (urn: string): Promise<{ elements: Array<IInstitutionalMemory> }> =>
  getJSON({ url: datasetInstitutionalMemoryByUrn(urn) });

/**
 * Returns a view of the full list of wiki-type url links to documents related to a dataset. This should be how the
 * list appears after any changes by the user (add or delete)
 * @param {string} urn - urn for the dataset
 * @param {Array<IInstitutionalMemory>} wikiLinks - related links snapshot
 */
export const writeDatasetInstitutionalMemory = (urn: string, wikiLinks: Array<IInstitutionalMemory>): Promise<void> =>
  postJSON({
    url: datasetInstitutionalMemoryByUrn(urn),
    data: { elements: wikiLinks }
  });
