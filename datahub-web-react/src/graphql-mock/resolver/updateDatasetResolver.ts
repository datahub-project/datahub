import { Dataset, DatasetUpdateInput } from '../../types.generated';
import { findDatasetByURN } from '../fixtures/searchResult/datasetSearchResult';
import { updateEntityLink, updateEntityOwners, updateEntityTag } from '../mutationHelper';

type UpdateDataset = {
    data: { updateDataset: Dataset };
};

export const updateDatasetResolver = {
    updateDataset({ variables: { urn, input } }): UpdateDataset {
        const { ownership, globalTags, institutionalMemory }: DatasetUpdateInput = input;
        const dataset = findDatasetByURN(urn);

        if (ownership) {
            updateEntityOwners({ entity: dataset, owners: ownership.owners });
        } else if (globalTags) {
            updateEntityTag({ entity: dataset, globalTags });
        } else if (institutionalMemory) {
            updateEntityLink({ entity: dataset, institutionalMemory });
        }

        return {
            data: {
                updateDataset: Object.assign(dataset, {
                    schema: null,
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
                }),
            },
        };
    },
};
