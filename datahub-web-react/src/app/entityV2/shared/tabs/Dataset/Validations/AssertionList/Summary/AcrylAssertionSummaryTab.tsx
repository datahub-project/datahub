import { Empty } from 'antd';
import { ArrowRight } from 'phosphor-react';
import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { AcrylAssertionSummaryCard } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/Summary/AcrylAssertionSummaryCard';
import { AcrylAssertionsSummaryTabLoading } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/Summary/AcrylAssertionsSummaryLoading';
import { getAssertionGroupsByDisplayOrder } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import { AssertionGroup } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylTypes';
import {
    AssertionWithMonitorDetails,
    createAssertionGroups,
    tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { useGetValidationsTab } from '@app/entityV2/shared/tabs/Dataset/Validations/useGetValidationsTab';
import { Button } from '@src/alchemy-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import {
    SEPARATE_SIBLINGS_URL_PARAM,
    combineEntityDataWithSiblings,
    useIsSeparateSiblingsMode,
} from '@src/app/entity/shared/siblingUtils';
import { useGetDatasetAssertionsWithMonitorsQuery } from '@src/graphql/monitor.generated';

const AcrylAssertionSummaryContainer = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
    padding: 16px;
    row-gap: 24px;
    column-gap: 24px;
    overflow: auto;
`;

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
`;

export const AcrylAssertionSummaryTab = () => {
    const { urn } = useEntityData();
    const history = useHistory();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const { pathname } = useLocation();
    const { basePath } = useGetValidationsTab(pathname, ['Summary']);

    const [groupedAssertions, setGroupedAssertions] = useState<AssertionGroup[]>([]);

    const { data, loading } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
        const assertionsWithMonitorsDetails: AssertionWithMonitorDetails[] =
            tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
        const assertionGroup = createAssertionGroups(assertionsWithMonitorsDetails);
        const orderedAssertionGroups = getAssertionGroupsByDisplayOrder(assertionGroup);
        setGroupedAssertions(orderedAssertionGroups);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data, loading]);

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
        return (
            <EmptyContainer>
                <Empty description="No assertions created yet." image={Empty.PRESENTED_IMAGE_SIMPLE} />
                <Button
                    onClick={() =>
                        history.replace(`${basePath}/List?${SEPARATE_SIBLINGS_URL_PARAM}=${isHideSiblingMode}`)
                    }
                >
                    Go to Assertions
                    <ArrowRight />
                </Button>
            </EmptyContainer>
        );
    };
    return <>{renderSummaryTab()}</>;
};
