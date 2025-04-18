import React, { useEffect, useState } from 'react';
import { Empty, message } from 'antd';
import styled from 'styled-components';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';
import { DataContract, EntityPrivileges } from '@src/types.generated';
import { TableLoadingSkeleton } from '@src/app/entityV2/shared/TableLoadingSkeleton';

import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../../graphql/monitor.generated';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '../../../../useIsSeparateSiblingsMode';
import { AssertionWithMonitorDetails, tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery } from '../acrylUtils';
import { combineEntityDataWithSiblings } from '../../../../../../entity/shared/siblingUtils';
import { getFilteredTransformedAssertionData } from './utils';
import { AssertionMonitorBuilderDrawer } from '../assertion/builder/AssertionMonitorBuilderDrawer';
import { createCachedAssertionWithMonitor, updateDatasetAssertionsCache } from '../acrylCacheUtils';
import { AssertionTable, AssertionListFilter, EntityStagedForAssertion } from './types';
import { AssertionListTitleContainer } from './AssertionListTitleContainer';
import { AcrylAssertionListFilters } from './AcrylAssertionListFilters';
import { AcrylAssertionListTable } from './AcrylAssertionListTable';
import { ASSERTION_DEFAULT_FILTERS, ASSERTION_DEFAULT_RAW_DATA } from './constant';
import { useOpenAssertionBuilder } from '../assertion/builder/hooks';

const AssertionListContainer = styled.div`
    margin: 0px 20px;
`;

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const { urn, entityData, entityType } = useEntityData();

    const [authorAssertionForEntity, setAuthorAssertionForEntity] = useState<EntityStagedForAssertion>();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [visibleAssertions, setVisibleAssertions] = useState<AssertionTable>({
        ...ASSERTION_DEFAULT_RAW_DATA,
    });
    // TODO we need to create setter function to set the filter as per the filter component
    const [selectedFilters, setSelectedFilters] = useState<AssertionListFilter>(ASSERTION_DEFAULT_FILTERS);

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
        const filteredAssertionData: AssertionTable = getFilteredTransformedAssertionData(assertions, selectedFilters);
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
    }, [selectedFilters]);

    const handleFilterChange = (filter: any) => {
        setSelectedFilters(filter);
    };

    const { privileges } = data?.dataset || {};
    const canEditMonitors = privileges?.canEditMonitors || false;
    const canEditAssertions = privileges?.canEditAssertions || false;
    const canEditSqlAssertionMonitors = privileges?.canEditSqlAssertionMonitors || false;

    const renderListTable = () => {
        if (loading) {
            return <TableLoadingSkeleton />;
        }
        if ((visibleAssertions?.assertions || []).length > 0) {
            return (
                <AcrylAssertionListTable
                    contract={contract}
                    assertionData={visibleAssertions}
                    filter={selectedFilters}
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

    useOpenAssertionBuilder(() => {
        if (!entityData?.platform) {
            message.open({
                content:
                    'Could not find platform details for this asset. Please contact support if this issue persists.',
                type: 'error',
            });
            return;
        }
        setAuthorAssertionForEntity({
            entityType,
            platform: entityData.platform,
            urn,
        });
    });

    return (
        <>
            <AssertionListTitleContainer
                privileges={privileges as EntityPrivileges}
                onCreateAssertion={(params: EntityStagedForAssertion) => setAuthorAssertionForEntity(params)}
            />
            <AssertionListContainer>
                {assertionMonitorData?.length > 0 && (
                    <AcrylAssertionListFilters
                        filterOptions={visibleAssertions?.filterOptions}
                        originalFilterOptions={visibleAssertions?.originalFilterOptions}
                        filteredAssertions={visibleAssertions}
                        selectedFilters={selectedFilters}
                        setSelectedFilters={setSelectedFilters}
                        handleFilterChange={handleFilterChange}
                    />
                )}
                {renderListTable()}
                {authorAssertionForEntity && (
                    <AssertionMonitorBuilderDrawer
                        entityUrn={authorAssertionForEntity.urn}
                        entityType={authorAssertionForEntity.entityType}
                        platform={authorAssertionForEntity.platform}
                        onSubmit={(assertion) => {
                            setAuthorAssertionForEntity(undefined);
                            updateDatasetAssertionsCache(
                                authorAssertionForEntity.urn,
                                createCachedAssertionWithMonitor(assertion),
                                client,
                            );
                            setTimeout(() => refetch(), 5000);
                        }}
                        onCancel={() => setAuthorAssertionForEntity(undefined)}
                    />
                )}
            </AssertionListContainer>
        </>
    );
};
