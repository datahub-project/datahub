import { Text, Timeline } from '@components';
import { CorpUser, Operation } from '@src/types.generated';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { pluralize } from '@src/app/shared/textUtil';
import { OPERATIONS_LIMIT } from '../constants';
import TimelineContent from './TimelineContent';
import TimelineDot from './TimelineDot';
import TimelineHeader from './TimelineHeader';
import TimelineSkeleton from './TimeLineSkeleton';
import usePrepareOperations from '../usePrepareOperations';

const TimelineWrapper = styled.div`
    margin-top: 20px;
    margin-left: 12px;
`;

type ChangeHistoryTimelineProps = {
    selectedDay?: string | null;
    operations: Operation[];
    users: CorpUser[];
    loading?: boolean;
};

export default function ChangeHistoryTimeline({ selectedDay, operations, users, loading }: ChangeHistoryTimelineProps) {
    const preparedOperations = usePrepareOperations(operations);

    const timelineItems = useMemo(() => {
        if (loading || users.length === 0) return [];

        return preparedOperations.map((operation) => {
            const foundUser = users.filter((user) => user.urn === operation.actor)?.[0];

            return {
                key: `operation-${operation.operationType}-${operation.lastUpdatedTimestamp}`,
                user: foundUser,
                operation,
            };
        });
    }, [preparedOperations, users, loading]);

    const numberOfOperations = preparedOperations.length;
    const numberOfOriginalOperations = operations.length;

    const renderTimeline = () => {
        if (loading) return <TimelineSkeleton />;

        if (operations.length === 0) return <Text color="gray">There are no operations for the selected day</Text>;

        return (
            <TimelineWrapper>
                <Timeline
                    items={timelineItems}
                    renderContent={(item) => <TimelineContent operation={item.operation} user={item.user} />}
                    renderDot={(item) => <TimelineDot user={item.user} />}
                />
            </TimelineWrapper>
        );
    };

    return (
        <>
            <TimelineHeader day={selectedDay} numberOfChanges={numberOfOperations} loading={loading} />

            {renderTimeline()}

            {numberOfOriginalOperations >= OPERATIONS_LIMIT && (
                <Text color="gray">
                    Truncated to show first {numberOfOperations} {pluralize(numberOfOperations, 'operation')}
                </Text>
            )}
        </>
    );
}
