import dayjs from 'dayjs';
import LocalizedFormat from 'dayjs/plugin/localizedFormat';
import relativeTime from 'dayjs/plugin/relativeTime';
import React, { useState } from 'react';
import styled from 'styled-components';

import { PreviousVersionModal } from '@app/entityV2/document/changeHistory/PreviousVersionModal';
import { ChangeMessage } from '@app/entityV2/document/changeHistory/changeMessages/ChangeMessageComponents';
import { extractChangeDetails, getActorDisplayName } from '@app/entityV2/document/changeHistory/utils/changeUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Popover, Text } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

import { DocumentChange, DocumentChangeType } from '@types';

dayjs.extend(LocalizedFormat);
dayjs.extend(relativeTime);

const Content = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 4px;
    top: -8px;
    margin-left: 11px;
`;

const TimeRow = styled.div`
    width: fit-content;
`;

const TimeText = styled(Text)`
    color: ${colors.gray[500]};
`;

interface DocumentChangeTimelineContentProps {
    change: DocumentChange;
    documentUrn: string;
}

/**
 * Timeline content component that displays a single change event.
 *
 * This component handles:
 * - Rendering the appropriate change message based on type
 * - Displaying the timestamp
 * - Opening the previous version modal for text changes
 */
export const DocumentChangeTimelineContent: React.FC<DocumentChangeTimelineContentProps> = ({
    change,
    documentUrn,
}) => {
    const entityRegistry = useEntityRegistry();
    const timestamp = dayjs(change.timestamp);
    const [isModalOpen, setIsModalOpen] = useState(false);

    // Extract actor name using utility function
    const actorName = getActorDisplayName(change.actor, entityRegistry);

    // Extract details using utility function
    const details = extractChangeDetails(change.details);

    return (
        <>
            <Content>
                {/* Render the change message based on type */}
                <ChangeMessage
                    changeType={change.changeType}
                    actorName={actorName}
                    actor={change.actor}
                    details={details}
                    description={change.description}
                    onSeeVersion={() => setIsModalOpen(true)}
                />

                {/* Timestamp with full date on hover */}
                <Popover content={timestamp.format('ll LTS')} placement="right">
                    <TimeRow>
                        <TimeText type="span" size="sm">
                            {timestamp.fromNow()}
                        </TimeText>
                    </TimeRow>
                </Popover>
            </Content>

            {/* Modal for viewing previous content version */}
            {change.changeType === DocumentChangeType.TextChanged && (
                <PreviousVersionModal
                    open={isModalOpen}
                    onClose={() => setIsModalOpen(false)}
                    previousContent={details.oldContent || ''}
                    documentUrn={documentUrn}
                />
            )}
        </>
    );
};
