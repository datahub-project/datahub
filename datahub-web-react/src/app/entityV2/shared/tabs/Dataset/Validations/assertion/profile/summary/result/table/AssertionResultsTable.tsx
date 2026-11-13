import { Timeline } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { AssertionResultDot } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/AssertionResultDot';
import { NoResultsSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/NoResultsSummary';
import { AssertionResultsLoadingItems } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionResultsLoadingItems';
import { AssertionResultsTableItem } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionResultsTableItem';
import { getResultColor } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

import { useGetAssertionRunsQuery } from '@graphql/assertion.generated';
import { Assertion, AssertionResultType, AssertionRunEvent } from '@types';

const Container = styled.div`
    margin-top: 20px;
`;

const StyledTimeline = styled.div`
    margin: 12px 4px;
`;

const ShowMoreButton = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
    cursor: pointer;
    :hover {
        text-decoration: underline;
    }
`;

type Props = {
    assertion: Assertion;
};

const DEFAULT_FETCH_COUNT = 25;
const INITIAL_VISIBLE_COUNT = 3;

export const AssertionResultsTable = ({ assertion }: Props) => {
    const theme = useTheme();
    const { t } = useTranslation('common.actions');
    const [count, setCount] = useState(INITIAL_VISIBLE_COUNT);
    // runEvents.total is the size of the returned page rather than a global count, so request one
    // extra event as a lookahead to decide whether another page exists.
    const { data, previousData, loading } = useGetAssertionRunsQuery({
        variables: {
            assertionUrn: assertion.urn,
            limit: count + 1,
        },
        fetchPolicy: 'cache-first',
    });
    // Keep the already-loaded runs on screen while the next page loads instead of flashing the skeleton.
    const fetchedRuns = (data ?? previousData)?.assertion?.runEvents?.runEvents || [];
    const visibleRuns = fetchedRuns.slice(0, count);
    const showMore = !loading && fetchedRuns.length > count;
    const showLoadingSkeleton = loading && fetchedRuns.length === 0;

    const timelineItems = visibleRuns.map((run) => {
        return {
            dot: (
                <div style={{ paddingTop: 12 }}>
                    <AssertionResultDot run={run as AssertionRunEvent} />
                </div>
            ),
            color: getResultColor(theme, run.result?.type as AssertionResultType),
            children: <AssertionResultsTableItem assertion={assertion} run={run as AssertionRunEvent} />,
            key: run.timestampMillis,
        };
    });

    return (
        <Container>
            <StyledTimeline>
                {showLoadingSkeleton ? (
                    <AssertionResultsLoadingItems />
                ) : (
                    timelineItems.map((item) => (
                        <Timeline.Item key={item.key} dot={item.dot} color={item.color}>
                            {item.children}
                        </Timeline.Item>
                    ))
                )}
            </StyledTimeline>
            {!loading && visibleRuns.length === 0 && <NoResultsSummary />}
            {showMore && (
                <ShowMoreButton
                    onClick={() => {
                        setCount((currentCount) => currentCount + DEFAULT_FETCH_COUNT);
                    }}
                >
                    {t('showMoreCapitalized')}
                </ShowMoreButton>
            )}
        </Container>
    );
};
