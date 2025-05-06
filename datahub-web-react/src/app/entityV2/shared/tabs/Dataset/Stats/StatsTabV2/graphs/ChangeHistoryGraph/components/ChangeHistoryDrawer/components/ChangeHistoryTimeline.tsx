import { Text, Timeline } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import TimelineSkeleton from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/TimeLineSkeleton';
import TimelineContent from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/TimelineContent';
import TimelineDot from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/TimelineDot';
import TimelineHeader from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/TimelineHeader';
import { OPERATIONS_LIMIT } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/constants';
import usePrepareOperations from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/usePrepareOperations';
import { pluralize } from '@src/app/shared/textUtil';
import { CorpUser, Operation } from '@src/types.generated';

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
