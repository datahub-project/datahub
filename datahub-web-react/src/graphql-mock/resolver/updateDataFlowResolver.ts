import { findDataFlowByURN } from '@graphql-mock/fixtures/searchResult/dataFlowSearchResult';
import { updateEntityOwners, updateEntityTag } from '@graphql-mock/mutationHelper';
import { DataFlow, DataFlowUpdateInput } from '@types';

type UpdateDataFlow = {
    data: { updateDataFlow: DataFlow };
};

export const updateDataFlowResolver = {
    updateDataFlow({ variables: { urn, input } }): UpdateDataFlow {
        const { globalTags, ownership }: DataFlowUpdateInput = input;
        const dataFlow = findDataFlowByURN(urn);

        if (ownership) {
            updateEntityOwners({ entity: dataFlow, owners: ownership?.owners });
        } else if (globalTags) {
            updateEntityTag({ entity: dataFlow, globalTags });
        }

        return {
            data: {
                updateDataFlow: Object.assign(dataFlow, {
                    info: {
                        ...dataFlow.info,
                        externalUrl: 'https://airflow.demo.datahub.com/tree?dag_id=datahub_analytics_refresh',
                        inputs: [],
                        customProperties: [],
                    },
                }),
            },
        };
    },
};
