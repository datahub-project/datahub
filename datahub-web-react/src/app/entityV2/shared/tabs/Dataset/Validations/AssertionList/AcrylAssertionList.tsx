import React, { useEffect, useState } from 'react';
import { Button, Tooltip, Typography } from 'antd';

import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../../graphql/monitor.generated';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '../../../../useIsSeparateSiblingsMode';
import { AssertionWithMonitorDetails, tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery } from '../acrylUtils';
import { combineEntityDataWithSiblings } from '../../../../../../entity/shared/siblingUtils';
import styled from 'styled-components';
import { getFilteredTransformedAssertionData, transformAssertionData } from './utils';
import { AssertionListTable } from './AssertionListTable';
import { PlusOutlined } from '@ant-design/icons';
import TabToolbar from '@src/app/entity/shared/components/styled/TabToolbar';
import { useAppConfig } from '@src/app/useAppConfig';
import { AssertionMonitorBuilderDrawer } from '../assertion/builder/AssertionMonitorBuilderDrawer';
import { createCachedAssertionWithMonitor, updateDatasetAssertionsCache } from '../acrylCacheUtils';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';
import { AcrylAssertionsSummaryLoading } from '../AcrylAssertionsSummaryLoading';
import { AssertionTableType, IFilter } from './types';
import { DataContract } from '@src/types.generated';

const AssertionTitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin: 20px;
    height: 50px;
    div {
        border-bottom: 0px;
    }
`;

const AssertionListTitle = styled(Typography.Title)`
    && {
        margin-bottom: 0px;
    }
`;

const CreateButton = styled(Button)`
    &&& {
        background-color: #5c3fd1;
        height: 40px;
        color: white;
        justify-content: center;
        align-items: center;
        border-radius: 5px;
        &:disabled {
            background-color: #e0e0e0 !important;
            height: 40px;
            color: #a0a0a0;
            opacity: 0.8;
        }
    }
`;
/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const { urn, entityData, entityType } = useEntityData();
    const { config } = useAppConfig();

    const [showAssertionBuilder, setShowAssertionBuilder] = useState(false);

    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [visibleAssertions, setVisibleAssertions] = useState<AssertionTableType>();
    // TODO we need to create setter function to set the filter as per the filter component
    const [filter] = useState<IFilter>({
        sortBy: '',
        groupBy: 'type',
        filterCriteria: {
            searchText: '',
            status: [],
            type: [],
            tags: [],
            columns: [],
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

    const assertionMonitorsEnabled = config?.featureFlags?.assertionMonitorsEnabled || false;
    const contract: DataContract = contractData?.dataset?.contract as DataContract;

    useEffect(() => {
        const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
        const assertionsWithMonitorsDetails: AssertionWithMonitorDetails[] =
            tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
        setAssertionMonitorData(assertionsWithMonitorsDetails);
        getFilteredAssertions(assertionsWithMonitorsDetails);
    }, [data]);

    // get filtered Assertion as per the filter object
    const getFilteredAssertions = (assertions: AssertionWithMonitorDetails[]) => {
        const filteredAssertionData: AssertionTableType = getFilteredTransformedAssertionData(assertions, filter);
        setVisibleAssertions(filteredAssertionData);
    };

    useEffect(() => {
        // after filter change need to get filtered assertions
        if (assertionMonitorData?.length > 0) {
            getFilteredAssertions(assertionMonitorData);
        }
    }, [filter]);

    const isSiblingMode = (entityData?.siblingsSearch?.total && !isHideSiblingMode) || false;
    const isSiblingModeMessage = (
        <>
            You cannot create an assertion for a group of assets. <br />
            <br />
            To create an assertion for a specific asset in this group, navigate to them using the <b>
                Composed Of
            </b>{' '}
            sidebar section below.
        </>
    );

    const isNotAllowedToCreateAssertionMessage = 'You do not have permission to create an assertion for this asset';

    /* We do not enable the create button if the user does not have the privilege, OR if sibling mode is enabled */
    const { privileges } = data?.dataset || {};
    const canEditAssertions = privileges?.canEditAssertions || false;
    const canEditMonitors = privileges?.canEditMonitors || false;
    const canEditSqlAssertionMonitors = privileges?.canEditSqlAssertionMonitors || false;
    const isAllowedToCreateAssertion = canEditAssertions && canEditMonitors;

    const disableCreateAssertion = !isAllowedToCreateAssertion || isSiblingMode;
    const disableCreateAssertionMessage = isSiblingMode ? isSiblingModeMessage : isNotAllowedToCreateAssertionMessage;

    const AssertionTitleSection = () => {
        return (
            <AssertionTitleContainer>
                <div className="left-section">
                    <AssertionListTitle level={4}>Assertions</AssertionListTitle>
                    <Typography.Text style={{ fontSize: 11 }}>
                        View and manage data quality checks for this table
                    </Typography.Text>
                </div>
                {assertionMonitorsEnabled && (
                    <TabToolbar>
                        <Tooltip
                            showArrow={false}
                            title={(disableCreateAssertion && disableCreateAssertionMessage) || null}
                        >
                            <CreateButton
                                onClick={() => !disableCreateAssertion && setShowAssertionBuilder(true)}
                                disabled={disableCreateAssertion}
                                id="create-assertion-btn-main"
                                className="create-assertion-button"
                            >
                                <PlusOutlined /> Create
                            </CreateButton>
                        </Tooltip>
                    </TabToolbar>
                )}
            </AssertionTitleContainer>
        );
    };

    return (
        <>
            <AssertionTitleSection></AssertionTitleSection>
            {loading ? (
                <AcrylAssertionsSummaryLoading />
            ) : (
                // TODO handle it in proper way - now added to work the expand the particular group if the assertion link is copied
                (visibleAssertions?.assertions || []).length > 0 && (
                    <AssertionListTable
                        contract={contract}
                        assertionData={visibleAssertions as AssertionTableType}
                        filter={filter}
                        refetch={() => {
                            setTimeout(() => {
                                refetch();
                                contractRefetch();
                            }, 500);
                        }}
                        canEditAssertions={canEditAssertions}
                        canEditMonitors={canEditMonitors}
                        canEditSqlAssertions={canEditSqlAssertionMonitors}
                    />
                )
            )}
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
