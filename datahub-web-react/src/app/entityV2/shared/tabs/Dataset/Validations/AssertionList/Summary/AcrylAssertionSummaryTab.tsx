import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { Empty } from 'antd';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import { useGetDatasetAssertionsWithRunEventsQuery } from '@src/graphql/dataset.generated';
import { Assertion } from '@src/types.generated';
import { createAssertionGroups, tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery } from '../../acrylUtils';
import { AcrylAssertionSummaryCard } from './AcrylAssertionSummaryCard';
import { AssertionGroup } from '../../acrylTypes';
import { getAssertionGroupsByDisplayOrder } from '../utils';
import { AcrylAssertionsSummaryTabLoading } from './AcrylAssertionsSummaryLoading';

const AcrylAssertionSummaryContainer = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
    padding: 16px;
    row-gap: 24px;
    column-gap: 24px;
    overflow: auto;
`;
export const AcrylAssertionSummaryTab = () => {
    const { urn } = useEntityData();

    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const [groupedAssertions, setGroupedAssertions] = useState<AssertionGroup[]>([]);

    const { data, loading } = useGetDatasetAssertionsWithRunEventsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
        const assertionsWithMonitorsDetails: Assertion[] =
            tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
        const assertionGroup = createAssertionGroups(assertionsWithMonitorsDetails);
        const orderedAssertionGroups = getAssertionGroupsByDisplayOrder(assertionGroup);
        setGroupedAssertions(orderedAssertionGroups);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]);

    const renderSummaryTab = () => {
        if (loading) {
            return <AcrylAssertionsSummaryTabLoading />;
        }
        if (groupedAssertions?.length > 0) {
            return (
                <AcrylAssertionSummaryContainer>
                    {groupedAssertions.map((group: AssertionGroup) => (
                        <AcrylAssertionSummaryCard group={group} />
                    ))}
                </AcrylAssertionSummaryContainer>
            );
        }
        return <Empty description="No assertions created yet." image={Empty.PRESENTED_IMAGE_SIMPLE} />;
    };
    return <>{renderSummaryTab()}</>;
};
