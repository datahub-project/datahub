import { Dataset, InstitutionalMemory, InstitutionalMemoryMetadata } from '../../types.generated';
import { findDatasetByURN } from '../fixtures/searchResult/datasetSearchResult';

type GetDataset = {
    data: { dataset: Dataset };
};

export const getDatasetResolver = {
    getDataset({ variables: { urn } }): GetDataset {
        const dataset = findDatasetByURN(urn);

        if (!dataset.institutionalMemory) {
            const baseElements: InstitutionalMemoryMetadata[] = [];
            const baseInstitutionalMemory: InstitutionalMemory = {
                elements: baseElements,
                __typename: 'InstitutionalMemory',
            };
            dataset.institutionalMemory = baseInstitutionalMemory;
        }

        return {
            data: {
                dataset: Object.assign(dataset, {
                    schema: null,
                    editableProperties: null,
                    editableSchemaMetadata: null,
                    deprecation: null,
                    downstreamLineage: {
                        entities: [],
                        __typename: 'DownstreamEntityRelationships',
                    },
                    upstreamLineage: {
                        entities: [],
                        __typename: 'UpstreamEntityRelationships',
                    },
                    glossaryTerms: null,
                    institutionalMemory: null,
                    usageStats: null,
                }),
            },
        };
    },
};
