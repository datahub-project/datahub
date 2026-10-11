import { Button } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretUp } from '@phosphor-icons/react/dist/csr/CaretUp';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t: tc } = useTranslation('common.actions');
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
                        icon={{ icon: CaretUp }}
                        iconPosition="right"
                        onClick={() => setIsExpanded(false)}
                    >
                        {tc('showLess')}
                    </Button>
                </ExpandCollapseButtonWrapper>
            );
        }

        return (
            <ExpandCollapseButtonWrapper>
                <Button
                    color="gray"
                    variant="link"
                    icon={{ icon: CaretDown }}
                    iconPosition="right"
                    onClick={() => setIsExpanded(true)}
                >
                    {tc('viewAllCount', { count: announcements.length })}
                </Button>
            </ExpandCollapseButtonWrapper>
        );
    }, [announcements, isExpanded, tc]);

    return (
        <>
            {finalAnnouncements.map((announcement) => (
                <AnnouncementCard key={announcement.urn} announcement={announcement} onDismiss={onDismiss} />
            ))}
            {renderExpandCollapseButton()}
        </>
    );
}
