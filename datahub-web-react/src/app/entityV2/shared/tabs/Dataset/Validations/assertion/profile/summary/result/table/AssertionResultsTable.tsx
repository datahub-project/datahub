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
    const { data, loading } = useGetAssertionRunsQuery({
        variables: {
            assertionUrn: assertion.urn,
            limit: count + 1,
        },
        fetchPolicy: 'cache-first',
    });
    // The API total is the returned page length. Probe one extra result instead.
    const runs = data?.assertion?.runEvents?.runEvents || [];
    const visibleRuns = runs.slice(0, count);
    const showMore = !loading && runs.length > count;

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
                {loading ? (
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
