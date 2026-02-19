import { findDataFlowByURN } from '@graphql-mock/fixtures/searchResult/dataFlowSearchResult';
import { DataFlow } from '@types';

type GetDataFlow = {
    data: { dataFlow: DataFlow };
};

export const getDataFlowResolver = {
    getDataFlow({ variables: { urn } }): GetDataFlow {
        const dataFlow = findDataFlowByURN(urn) as DataFlow;
        return {
            data: {
                dataFlow: Object.assign(dataFlow, {
                    info: {
                        ...dataFlow.info,
                        externalUrl: 'https://airflow.demo.datahubproject.io/tree?dag_id=datahub_analytics_refresh',
                        inputs: [],
                        customProperties: [],
                    },
                }),
            },
        };
    },
};
