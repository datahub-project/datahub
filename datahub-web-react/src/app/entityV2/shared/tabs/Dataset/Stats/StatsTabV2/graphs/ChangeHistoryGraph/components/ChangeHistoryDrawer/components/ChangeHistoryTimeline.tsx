import { Text, Timeline } from '@components';
import { CorpUser, Operation } from '@src/types.generated';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { OPERATIONS_LIMIT } from '../constants';
import TimelineContent from './TimelineContent';
import TimelineDot from './TimelineDot';
import TimelineHeader from './TimelineHeader';
import TimelineSkeleton from './TimeLineSkeleton';

const TimelineWrapper = styled.div`
    margin-top: 20px;
    margin-left: 12px;
`;

type ChangeHistoryTimelineProps = {
    selectedDay?: string;
    operations: Omit<Operation, 'lastUpdatedTimestamp'>[];
    users: CorpUser[];
    loading?: boolean;
};

export default function ChangeHistoryTimeline({ selectedDay, operations, users, loading }: ChangeHistoryTimelineProps) {
    const timelineItems = useMemo(() => {
        if (loading || users.length === 0) return [];

        return operations.map((operation) => {
            const foundUser = users.filter((user) => user.urn === operation.actor)?.[0];

            return {
                key: `operation-${operation.operationType}-${operation.timestampMillis}`,
                user: foundUser,
                operation,
            };
        });
    }, [operations, users, loading]);

    const renderTimeline = () => {
        if (loading) return <TimelineSkeleton />;

        if (operations.length === 0) return <Text color="gray">There are no any operations for the selected day</Text>;

        return (
            <TimelineWrapper>
                <Timeline
                    items={timelineItems}
                    renderContent={(item) => <TimelineContent operation={item.operation} user={item.user} />}
                    renderDot={(item) => <TimelineDot user={item.user} /> || undefined}
                />
            </TimelineWrapper>
        );
    };

    return (
        <>
            <TimelineHeader day={selectedDay} numberOfChanges={operations.length} loading={loading} />

            {renderTimeline()}

            {operations.length >= OPERATIONS_LIMIT && (
                <Text color="gray">Truncated to show first {OPERATIONS_LIMIT} operations</Text>
            )}
        </>
    );
}
