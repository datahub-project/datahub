import { DataJob, DataJobUpdateInput } from '../../types.generated';
import { findDataJobByURN } from '../fixtures/searchResult/dataJobSearchResult';
import { updateEntityOwners, updateEntityTag } from '../mutationHelper';

type UpdateDataJob = {
    data: { updateDataJob: DataJob };
};

export const updateDataJobResolver = {
    updateDataJob({ variables: { input } }): UpdateDataJob {
        const { urn, ownership, globalTags }: DataJobUpdateInput = input;
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
