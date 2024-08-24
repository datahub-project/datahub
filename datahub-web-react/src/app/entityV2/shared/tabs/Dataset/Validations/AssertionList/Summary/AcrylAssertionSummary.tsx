import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import { useGetDatasetAssertionsWithMonitorsQuery } from '@src/graphql/monitor.generated';
import {
    AssertionWithMonitorDetails,
    createAssertionGroups,
    tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery,
} from '../../acrylUtils';
import { AcrylAssertionSummaryCard } from './AcrylAssertionSummaryCard';
import { AssertionGroup } from '../../acrylTypes';
import { AcrylAssertionsSummaryLoading } from '../../AcrylAssertionsSummaryLoading';

const AcrylAssertionSummaryContainer = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
    //   gap: 24px;
    padding: 24px;
    row-gap: 24px;
    column-gap: 24px;
`;

export const AcrylAssertionSummary = () => {
    const { urn } = useEntityData();

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
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]);
    return (
        <>
            {loading ? (
                <AcrylAssertionsSummaryLoading />
            ) : (
                <AcrylAssertionSummaryContainer>
                    {groupedAssertions.map((group: AssertionGroup) => (
                        <AcrylAssertionSummaryCard group={group} />
                    ))}
                </AcrylAssertionSummaryContainer>
            )}
        </>
    );
};
