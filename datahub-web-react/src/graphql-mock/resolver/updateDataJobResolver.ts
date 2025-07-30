import { findDataJobByURN } from '@graphql-mock/fixtures/searchResult/dataJobSearchResult';
import { updateEntityOwners, updateEntityTag } from '@graphql-mock/mutationHelper';
import { DataJob, DataJobUpdateInput } from '@types';

type UpdateDataJob = {
    data: { updateDataJob: DataJob };
};

export const updateDataJobResolver = {
    updateDataJob({ variables: { urn, input } }): UpdateDataJob {
        const { ownership, globalTags }: DataJobUpdateInput = input;
        const dataJob = findDataJobByURN(urn);

        if (ownership) {
            updateEntityOwners({ entity: dataJob, owners: ownership?.owners });
        } else if (globalTags) {
            updateEntityTag({ entity: dataJob, globalTags });
        }

        return {
            data: {
                updateDataJob: dataJob,
            },
        };
    },
};
