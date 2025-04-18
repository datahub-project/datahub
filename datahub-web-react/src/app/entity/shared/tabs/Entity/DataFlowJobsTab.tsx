import React, { useState } from 'react';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from './components/EntityList';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { useGetDataFlowChildJobsQuery } from '../../../../../graphql/dataFlow.generated';
import { SearchCfg } from '../../../../../conf';

interface Props {
    properties?: {
        urn: string;
    };
}

export const DataFlowJobsTab = ({ properties = { urn: '' } }: Props) => {
    const [page, setPage] = useState(1);
    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);

    const start: number = (page - 1) * numResultsPerPage;

    const { data, loading, error } = useGetDataFlowChildJobsQuery({
        variables: {
            urn: properties.urn,
            start,
            count: numResultsPerPage,
        },
    });

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const dataFlow = data && data?.dataFlow;
    const dataJobs = dataFlow?.childJobs?.relationships?.map((relationship) => relationship.entity);
    const entityRegistry = useEntityRegistry();
    const totalJobs = dataFlow?.childJobs?.total || 0;
    const pageSize = data?.dataFlow?.childJobs?.count || 0;
    const pageStart = data?.dataFlow?.childJobs?.start || 0;
    const lastResultIndex = pageStart + pageSize > totalJobs ? totalJobs : pageStart + pageSize;
    const title = `Contains ${totalJobs} ${
        totalJobs === 1
            ? entityRegistry.getEntityName(EntityType.DataJob)
            : entityRegistry.getCollectionName(EntityType.DataJob)
    }`;
    return (
        <EntityList
            title={title}
            type={EntityType.DataJob}
            entities={dataJobs || []}
            showPagination
            loading={loading}
            error={error}
            totalAssets={totalJobs}
            page={page}
            pageSize={numResultsPerPage}
            lastResultIndex={lastResultIndex}
            onChangePage={onChangePage}
            setNumResultsPerPage={setNumResultsPerPage}
        />
    );
};
