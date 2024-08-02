import React, { useEffect, useState } from 'react';
import { Button, Empty, Tooltip, TableProps, Table, Typography } from 'antd';

import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../../graphql/monitor.generated';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '../../../../useIsSeparateSiblingsMode';
import { AssertionWithMonitorDetails, tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery } from '../acrylUtils';
import { combineEntityDataWithSiblings } from '../../../../../../entity/shared/siblingUtils';
import styled from 'styled-components';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../../constants';
import { useBuildAssertionDescriptionLabels } from '../assertion/profile/summary/utils';
import { AssertionRunStatus } from '@src/types.generated';
import { getFilteredTransformedAssertionData, transformAssertionData } from './utils';
import { AssertionListTable } from './AssertionListTable';
import { PlusOutlined } from '@ant-design/icons';
import TabToolbar from '@src/app/entity/shared/components/styled/TabToolbar';
import { useAppConfig } from '@src/app/useAppConfig';
import { AssertionMonitorBuilderDrawer } from '../assertion/builder/AssertionMonitorBuilderDrawer';
import { createCachedAssertionWithMonitor, updateDatasetAssertionsCache } from '../acrylCacheUtils';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';

export type IFilter = {
    sortBy: string;
    groupBy: string;
    filterCriteria: {
        searchText: string;
        status: string[];
        type: string[];
        tags: string[];
        columns: string[];
    };
};

const dummyFilterObject: IFilter = {
    sortBy: '',
    groupBy: 'type',
    filterCriteria: {
        searchText: '',
        status: [],
        type: [],
        tags: [],
        columns: [],
    },
};

const AssertionConinter = styled.div``;
const AssertionHeader = styled.div``;
const AssertionTitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin: 20px;
    height: 50px;
    .create-button {
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        justify-content: center;
        align-items: center;
        color: white;
        height: 40px;
        border-radius: 5px;
    }
`;

const AssertionListTitle = styled(Typography.Title)`
    && {
        margin-bottom: 0px;
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
    const [visibleAssertions, setVisibleAssertions] = useState<any>({ allAssertions: [] });
    const [filter, setFilter] = useState<IFilter>({ ...dummyFilterObject });
    const [assertionMonitorData, setAssertionMonitorData] = useState<any[]>([]);

    const { data, refetch, client, loading } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });
    const { data: contractData, refetch: contractRefetch } = useGetDatasetContractQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const assertionMonitorsEnabled = config?.featureFlags?.assertionMonitorsEnabled || false;
    const contract = contractData?.dataset?.contract as any;

    useEffect(() => {
        const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
        const assertionsWithMonitorsDetails: AssertionWithMonitorDetails[] =
            tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
        setAssertionMonitorData(assertionsWithMonitorsDetails);
        const transformedAssertions = transformAssertionData(assertionsWithMonitorsDetails);
        setVisibleAssertions(transformedAssertions);
        setFilter({ ...dummyFilterObject });
    }, [data]);

    useEffect(() => {
        const filteredAssertionData = getFilteredTransformedAssertionData(assertionMonitorData, filter);
        setVisibleAssertions(filteredAssertionData);
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

    const isAllowedToCreateAssertion =
        (data?.dataset?.privileges?.canEditAssertions || false) &&
        (data?.dataset?.privileges?.canEditMonitors || false);
    const isNotAllowedToCreateAssertionMessage = 'You do not have permission to create an assertion for this asset';

    /* We do not enable the create button if the user does not have the privilege, OR if sibling mode is enabled */
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
                            <Button
                                type="text"
                                onClick={() => !disableCreateAssertion && setShowAssertionBuilder(true)}
                                disabled={disableCreateAssertion}
                                id="create-assertion-btn-main"
                                className="create-button"
                            >
                                <PlusOutlined /> Create
                            </Button>
                        </Tooltip>
                    </TabToolbar>
                )}
            </AssertionTitleContainer>
        );
    };

    return (
        <>
            <AssertionConinter>
                <AssertionHeader>
                    <AssertionTitleSection></AssertionTitleSection>
                </AssertionHeader>
                <AssertionListTable
                    contract={contract}
                    assertionData={visibleAssertions}
                    filterOptions={filter}
                    refetch={() => {
                        refetch();
                        contractRefetch();
                    }}
                />
            </AssertionConinter>
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
