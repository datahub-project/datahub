import { DataJob } from '../../types.generated';
import { findDataJobByURN } from '../fixtures/searchResult/dataJobSearchResult';

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
                        externalUrl: 'https://airflow.demo.datahubproject.io/tree?dag_id=datahub_analytics_refresh',
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
