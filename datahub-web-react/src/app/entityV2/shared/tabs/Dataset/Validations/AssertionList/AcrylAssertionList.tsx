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
import { TableLoadingSkeleton } from '@src/app/entityV2/shared/TableLoadingSkeleton';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';
import { DataContract, EntityPrivileges } from '@src/types.generated';

import { useGetDatasetAssertionsWithMonitorsQuery } from '@graphql/monitor.generated';

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
