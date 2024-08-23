import React, { useEffect, useState } from 'react';
import { AcrylAssertionSummaryCard } from './AcrylAssertionSummaryCard';
import styled from 'styled-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import { useGetDatasetAssertionsWithMonitorsQuery } from '@src/graphql/monitor.generated';
import {
    AssertionWithMonitorDetails,
    createAssertionGroups,
    tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery,
} from '../acrylUtils';
import { AssertionGroup } from '../acrylTypes';

const AcrylAssertionSummaryContainer = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
    //   gap: 24px;
    padding: 24px;
    row-gap: 24px;
    column-gap: 24px;
`;

const CardWrapper = styled.div`
    display: flex;
    justify-content: center;
`;

export const AcrylAssertionSummary = () => {
    const { urn, entityData, entityType } = useEntityData();

    const isHideSiblingMode = useIsSeparateSiblingsMode();

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
        setGroupedAssertions(assertionGroup);
        console.log('assertionGroup>>>>', assertionGroup);

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]);
    return (
        <AcrylAssertionSummaryContainer>
            {groupedAssertions.map((group: AssertionGroup) => (
                <AcrylAssertionSummaryCard group={group} />
            ))}
        </AcrylAssertionSummaryContainer>
    );
};
