import { Empty, message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';
import { AcrylAssertionListFilters } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListFilters';
import { AcrylAssertionListTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListTable';
import { AssertionListTitleContainer } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AssertionListTitleContainer';
import {
    ASSERTION_DEFAULT_FILTERS,
    ASSERTION_DEFAULT_RAW_DATA,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import {
    AssertionListFilter,
    AssertionTable,
    EntityStagedForAssertion,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { getFilteredTransformedAssertionData } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import {
    createCachedAssertionWithMonitor,
    updateDatasetAssertionsCache,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylCacheUtils';
import {
    AssertionWithMonitorDetails,
    tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionMonitorBuilderDrawer } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/AssertionMonitorBuilderDrawer';
import { useOpenAssertionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { EMBEDDED_EXECUTOR_POOL_NAME } from '@app/shared/constants';
import { TableLoadingSkeleton } from '@src/app/entityV2/shared/TableLoadingSkeleton';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';
import { DataContract, EntityPrivileges } from '@src/types.generated';

import { useIngestionSourceForEntityQuery } from '@graphql/ingestion.generated';
import { useGetDatasetAssertionsWithMonitorsQuery } from '@graphql/monitor.generated';

const AssertionListContainer = styled.div`
    display: flex;
    height: 100%;
    flex-direction: column;
    margin: 16px;
    flex: 1;
    overflow: hidden;
`;

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const { urn, entityData, entityType } = useEntityData();
    const { data: ingestionSourceData, loading: ingestionSourceLoading } = useIngestionSourceForEntityQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const [authorAssertionForEntity, setAuthorAssertionForEntity] = useState<EntityStagedForAssertion>();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [visibleAssertions, setVisibleAssertions] = useState<AssertionTable>({
        ...ASSERTION_DEFAULT_RAW_DATA,
    });
    // TODO we need to create setter function to set the filter as per the filter component
    const [selectedFilters, setSelectedFilters] = useState<AssertionListFilter>(ASSERTION_DEFAULT_FILTERS);

    const [assertionMonitorData, setAssertionMonitorData] = useState<AssertionWithMonitorDetails[]>([]);

    const {
        data,
        refetch,
        client,
        loading: assertionLoading,
    } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });
    const {
        data: contractData,
        refetch: contractRefetch,
        loading: contractLoading,
    } = useGetDatasetContractQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const contract = contractData?.dataset?.contract as DataContract | undefined;

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
        if (assertionLoading || ingestionSourceLoading || contractLoading) {
            return <TableLoadingSkeleton />;
        }
        if ((visibleAssertions?.assertions || []).length > 0) {
            const maybeExecutorId = ingestionSourceData?.ingestionSourceForEntity?.config?.executorId;
            const isReachable =
                !maybeExecutorId || maybeExecutorId.toLowerCase().startsWith(EMBEDDED_EXECUTOR_POOL_NAME);
            return (
                <AcrylAssertionListTable
                    contract={contract}
                    assertionData={visibleAssertions}
                    refetch={() => {
                        refetch();
                        contractRefetch();
                    }}
                    isEntityReachable={isReachable}
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
            <AssertionListContainer>
                <AssertionListTitleContainer
                    privileges={privileges as EntityPrivileges}
                    onCreateAssertion={(params: EntityStagedForAssertion) => setAuthorAssertionForEntity(params)}
                />

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
