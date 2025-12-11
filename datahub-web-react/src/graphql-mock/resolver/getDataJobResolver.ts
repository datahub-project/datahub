/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { findDataJobByURN } from '@graphql-mock/fixtures/searchResult/dataJobSearchResult';
import { DataJob } from '@types';

type GetJobFlow = {
    data: { dataJob: DataJob };
};

export const getDataJobResolver = {
    getDataJob({ variables: { urn } }): GetJobFlow {
        const dataJob = findDataJobByURN(urn) as DataJob;
        return {
            data: {
                dataJob: Object.assign(dataJob, {
                    info: {
                        ...dataJob.info,
                        externalUrl: 'https://airflow.demo.datahub.com/tree?dag_id=datahub_analytics_refresh',
                        inputs: [],
                        customProperties: [],
                    },
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
