import { Text, Timeline } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { DocumentChangeTimelineContent } from '@app/entityV2/document/changeHistory/DocumentChangeTimelineContent';
import { DocumentChangeTimelineDot } from '@app/entityV2/document/changeHistory/DocumentChangeTimelineDot';
import Loading from '@app/shared/Loading';

import { DocumentChange } from '@types';

const TimelineWrapper = styled.div`
    margin-top: 20px;
    margin-left: 12px;
`;

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    padding: 40px;
    height: 300px;
`;

interface DocumentHistoryTimelineProps {
    documentUrn: string;
    changes: DocumentChange[];
    loading?: boolean;
}

export const DocumentHistoryTimeline: React.FC<DocumentHistoryTimelineProps> = ({ documentUrn, changes, loading }) => {
    const timelineItems = useMemo(() => {
        return changes.map((change, index) => ({
            key: `change-${change.changeType}-${change.timestamp}-${index}`,
            change,
            documentUrn,
        }));
    }, [changes, documentUrn]);

    if (loading) {
        return (
            <LoadingWrapper>
                <Loading />
            </LoadingWrapper>
        );
    }

    if (changes.length === 0) {
        return (
            <LoadingWrapper>
                <Text color="gray">No change history yet!</Text>
            </LoadingWrapper>
        );
    }

    return (
        <TimelineWrapper>
            <Timeline
                items={timelineItems}
                renderContent={(item) => (
                    <DocumentChangeTimelineContent change={item.change} documentUrn={item.documentUrn} />
                )}
                renderDot={(item) => <DocumentChangeTimelineDot change={item.change} />}
            />
        </TimelineWrapper>
    );
};
