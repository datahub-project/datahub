import React, { useState } from 'react';

import styled from 'styled-components';
import { Timeline } from 'antd';

import {
    Assertion,
    AssertionResultType,
    AssertionRunEvent,
    Monitor,
} from '../../../../../../../../../../../types.generated';
import { useGetAssertionRunsQuery } from '../../../../../../../../../../../graphql/assertion.generated';
import { ANTD_GRAY } from '../../../../../../../../constants';
import { getResultColor } from '../../../../../assertionUtils';
import { AssertionResultsTableItem } from './AssertionResultsTableItem';
import { AssertionResultsLoadingItems } from './AssertionResultsLoadingItems';
import { NoResultsSummary } from '../../NoResultsSummary';
import { AssertionResultDot } from '../../../shared/AssertionResultDot';
import { useAssertionPredictionItem } from './utils.saas';

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
    monitor?: Monitor;
};

const DEFAULT_FETCH_COUNT = 25;
const INITIAL_VISIBLE_COUNT = 3;

export const AssertionResultsTable = ({ assertion, monitor }: Props) => {
    const [count, setCount] = useState(INITIAL_VISIBLE_COUNT);
    const { data, loading } = useGetAssertionRunsQuery({
        variables: {
            assertionUrn: assertion.urn,
            limit: count,
        },
        fetchPolicy: 'cache-first',
    });
    const visibleRuns = data?.assertion?.runEvents?.runEvents || [];
    const total = data?.assertion?.runEvents?.total || 0;
    const showMore = count <= total;

    const maybeAssertionPredictionItem = useAssertionPredictionItem(assertion, monitor);

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
                {loading && !timelineItems.length ? (
                    <AssertionResultsLoadingItems />
                ) : (
                    [
                        // SaaS only //
                        maybeAssertionPredictionItem ? (
                            <Timeline.Item
                                key={maybeAssertionPredictionItem.key}
                                dot={maybeAssertionPredictionItem.dot}
                                color={maybeAssertionPredictionItem.color}
                            >
                                {maybeAssertionPredictionItem.children}
                            </Timeline.Item>
                        ) : null,
                        // end SaaS only //
                        timelineItems.map((item) => (
                            <Timeline.Item key={item.key} dot={item.dot} color={item.color}>
                                {item.children}
                            </Timeline.Item>
                        )),
                    ]
                )}
            </StyledTimeline>
            {showMore && (
                <ShowMoreButton
                    onClick={() => {
                        setCount((currentCount) => currentCount + DEFAULT_FETCH_COUNT);
                    }}
                >
                    Show more
                </ShowMoreButton>
            )}
        </Container>
    );
};
