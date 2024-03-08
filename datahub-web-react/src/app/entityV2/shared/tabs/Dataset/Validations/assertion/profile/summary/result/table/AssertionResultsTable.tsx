import React, { useState } from 'react';

import styled from 'styled-components';
import { Timeline } from 'antd';

import { Assertion, AssertionResultType, AssertionRunEvent } from '../../../../../../../../../../../types.generated';
import { useGetAssertionRunsQuery } from '../../../../../../../../../../../graphql/assertion.generated';
import { ANTD_GRAY } from '../../../../../../../../constants';
import { getResultColor } from '../../../../../assertionUtils';
import { AssertionResultsTableItem } from './AssertionResultsTableItem';
import { AssertionResultsLoadingItems } from './AssertionResultsLoadingItems';
import { NoResultsSummary } from '../../NoResultsSummary';
import { AssertionResultDot } from '../../../shared/AssertionResultDot';

const Container = styled.div`
    margin-top: 20px;
`;

const StyledTimeline = styled.div`
    margin: 12px 4px;
`;

const ShowMoreButton = styled.div`
    color: ${ANTD_GRAY[7]};
    cursor: pointer;
    :hover {
        text-decoration: underline;
    }
`;

type Props = {
    assertion: Assertion;
};

const DEFAULT_FETCH_COUNT = 25;
const DEFAULT_VISIBLE_COUNT = 3;

export const AssertionResultsTable = ({ assertion }: Props) => {
    const [count, setCount] = useState(DEFAULT_FETCH_COUNT);
    const [visible, setVisible] = useState(DEFAULT_VISIBLE_COUNT);
    const { data, loading } = useGetAssertionRunsQuery({
        variables: {
            assertionUrn: assertion.urn,
            limit: count,
        },
        fetchPolicy: 'cache-first',
    });
    const visibleRuns = data?.assertion?.runEvents?.runEvents?.slice(0, visible) || [];
    const total = data?.assertion?.runEvents?.total || 0;
    const showMore = visible < total;

    const timelineItems = visibleRuns.map((run) => {
        return {
            dot: (
                <div style={{ paddingTop: 12 }}>
                    <AssertionResultDot run={run as AssertionRunEvent} />
                </div>
            ),
            color: getResultColor(run.result?.type as AssertionResultType),
            children: <AssertionResultsTableItem assertion={assertion} run={run as AssertionRunEvent} />,
            key: run.timestampMillis,
        };
    });

    if (visibleRuns.length === 0) {
        return <NoResultsSummary />;
    }

    return (
        <Container>
            <StyledTimeline>
                {(loading && <AssertionResultsLoadingItems />) ||
                    timelineItems.map((item) => (
                        <Timeline.Item key={item.key} dot={item.dot} color={item.color}>
                            {item.children}
                        </Timeline.Item>
                    ))}
            </StyledTimeline>
            {showMore && (
                <ShowMoreButton
                    onClick={() => {
                        if (visible + DEFAULT_VISIBLE_COUNT > count) {
                            setCount(count + DEFAULT_FETCH_COUNT);
                        }
                        setVisible(visible + DEFAULT_VISIBLE_COUNT);
                    }}
                >
                    Show more
                </ShowMoreButton>
            )}
        </Container>
    );
};
