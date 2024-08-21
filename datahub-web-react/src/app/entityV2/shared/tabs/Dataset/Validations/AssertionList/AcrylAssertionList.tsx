import React, { useEffect, useState } from 'react';
import { Empty } from 'antd';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';
import { DataContract, EntityPrivileges } from '@src/types.generated';
import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../../graphql/monitor.generated';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '../../../../useIsSeparateSiblingsMode';
import { AssertionWithMonitorDetails, tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery } from '../acrylUtils';
import { combineEntityDataWithSiblings } from '../../../../../../entity/shared/siblingUtils';
import { getFilteredTransformedAssertionData } from './utils';
import { AssertionMonitorBuilderDrawer } from '../assertion/builder/AssertionMonitorBuilderDrawer';
import { createCachedAssertionWithMonitor, updateDatasetAssertionsCache } from '../acrylCacheUtils';
import { AcrylAssertionsSummaryLoading } from '../AcrylAssertionsSummaryLoading';
import { AssertionTable, AssertionListFilter } from './types';
import { AssertionListTitleContainer } from './AssertionListTitleContainer';
import { AcrylAssertionListFilters } from './AcrylAssertionListFilters';
import { AcrylAssertionListTable } from './AcrylAssertionListTable';

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const { urn, entityData, entityType } = useEntityData();

    const [showAssertionBuilder, setShowAssertionBuilder] = useState<boolean>(false);

    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [visibleAssertions, setVisibleAssertions] = useState<AssertionTable>({
        assertions: [],
        groupBy: { type: [], status: [] },
    });
    // TODO we need to create setter function to set the filter as per the filter component
    const [filter, setFilters] = useState<AssertionListFilter>({
        sortBy: '',
        groupBy: 'type',
        filterCriteria: {
            searchText: '',
            status: [],
            type: [],
            tags: [],
            columns: [],
            others: [],
        },
    });
    const [assertionMonitorData, setAssertionMonitorData] = useState<AssertionWithMonitorDetails[]>([]);

    const { data, refetch, client, loading } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });
    const { data: contractData, refetch: contractRefetch } = useGetDatasetContractQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const contract: DataContract = contractData?.dataset?.contract as DataContract;

    // get filtered Assertion as per the filter object
    const getFilteredAssertions = (assertions: AssertionWithMonitorDetails[]) => {
        const filteredAssertionData: AssertionTable = getFilteredTransformedAssertionData(assertions, filter);
        setVisibleAssertions(filteredAssertionData);
    };

    useEffect(() => {
        const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
        const assertionsWithMonitorsDetails: AssertionWithMonitorDetails[] =
            tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
        setAssertionMonitorData(assertionsWithMonitorsDetails);
        getFilteredAssertions(assertionsWithMonitorsDetails);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]);

    useEffect(() => {
        // after filter change need to get filtered assertions
        if (assertionMonitorData?.length > 0) {
            getFilteredAssertions(assertionMonitorData);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [filter]);

    const { privileges } = data?.dataset || {};
    const canEditMonitors = privileges?.canEditMonitors || false;
    const canEditAssertions = privileges?.canEditAssertions || false;
    const canEditSqlAssertionMonitors = privileges?.canEditSqlAssertionMonitors || false;

    const renderListTable = () => {
        if (loading) {
            return <AcrylAssertionsSummaryLoading />;
        }
        if ((visibleAssertions?.assertions || []).length > 0) {
            return (
                <AcrylAssertionListTable
                    contract={contract}
                    assertionData={visibleAssertions}
                    filter={filter}
                    refetch={() => {
                        refetch();
                        contractRefetch();
                    }}
                    canEditAssertions={canEditAssertions}
                    canEditMonitors={canEditMonitors}
                    canEditSqlAssertions={canEditSqlAssertionMonitors}
                />
            );
        }
        return <Empty description="No assertions have run" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
    };

    return (
        <>
            <AssertionListTitleContainer
                privileges={privileges as EntityPrivileges}
                setShowAssertionBuilder={setShowAssertionBuilder}
            />
            <AcrylAssertionListFilters
                filterOptions={visibleAssertions?.filterOptions}
                setFilters={setFilters}
                filter={filter}
                allAssertionCount={assertionMonitorData?.length || 0}
                filteredAssertions={visibleAssertions}
            />
            {renderListTable()}
            {showAssertionBuilder && (
                <AssertionMonitorBuilderDrawer
                    entityUrn={urn}
                    entityType={entityType}
                    platformUrn={entityData?.platform?.urn as string}
                    onSubmit={(assertion) => {
                        setShowAssertionBuilder(false);
                        updateDatasetAssertionsCache(urn, createCachedAssertionWithMonitor(assertion), client);
                        setTimeout(() => refetch(), 5000);
                    }}
                    onCancel={() => setShowAssertionBuilder(false)}
                />
            )}
        </>
    );
};
