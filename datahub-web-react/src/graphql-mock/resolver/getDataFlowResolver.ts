/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
                        externalUrl: 'https://airflow.demo.datahub.com/tree?dag_id=datahub_analytics_refresh',
                        inputs: [],
                        customProperties: [],
                    },
                }),
            },
        };
    },
};
