/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { AnnouncementCard } from '@app/homeV3/announcements/AnnouncementCard';

import { Post } from '@types';

const MAX_ANNOUNCEMENTS_TO_PREVIEW = 3;

const ExpandCollapseButtonWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    padding: 4px 6px;
`;

interface Props {
    announcements: Post[];
    onDismiss: (urn: string) => void;
}

export default function ExpandableAnnouncements({ announcements, onDismiss }: Props) {
    const [isExpanded, setIsExpanded] = useState<boolean>(false);

    const finalAnnouncements = useMemo(
        () => (isExpanded ? announcements : announcements.slice(0, MAX_ANNOUNCEMENTS_TO_PREVIEW)),
        [announcements, isExpanded],
    );

    const renderExpandCollapseButton = useCallback(() => {
        const isExpandable = announcements.length > MAX_ANNOUNCEMENTS_TO_PREVIEW;

        if (!isExpandable) return null;

        if (isExpanded) {
            return (
                <ExpandCollapseButtonWrapper>
                    <Button
                        color="gray"
                        variant="link"
                        icon={{ icon: 'CaretUp', source: 'phosphor' }}
                        iconPosition="right"
                        onClick={() => setIsExpanded(false)}
                    >
                        Show less
                    </Button>
                </ExpandCollapseButtonWrapper>
            );
        }

        return (
            <ExpandCollapseButtonWrapper>
                <Button
                    color="gray"
                    variant="link"
                    icon={{ icon: 'CaretDown', source: 'phosphor' }}
                    iconPosition="right"
                    onClick={() => setIsExpanded(true)}
                >
                    View all ({announcements.length})
                </Button>
            </ExpandCollapseButtonWrapper>
        );
    }, [announcements, isExpanded]);

    return (
        <>
            {finalAnnouncements.map((announcement) => (
                <AnnouncementCard key={announcement.urn} announcement={announcement} onDismiss={onDismiss} />
            ))}
            {renderExpandCollapseButton()}
        </>
    );
}
