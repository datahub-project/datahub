import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';

import colors from '@src/alchemy-components/theme/foundations/colors';
import { toLocalDateTimeString, toRelativeTimeString } from '@src/app/shared/time/timeUtils';

const PopoverContent = styled.div`
    color: ${colors.gray[500]};
    font-size: 0.8rem;
`;

const Title = styled.div`
    color: ${colors.gray[500]};
    border-bottom: none;
    font-size: 0.8rem;
    font-weight: 600;
`;

const InfoItemContent = styled.div`
    padding-top: 8px;
    width: 100px;
    overflow-wrap: break-word;
`;

const popoverStyles = {
    overlayInnerStyle: {
        borderRadius: '10px',
    },
    overlayStyle: {
        margin: '5px',
    },
};

interface TimestampProps {
    timestamp?: number;
    title: string;
    showPopover?: boolean;
}

export const TimestampPopover: React.FC<TimestampProps> = ({ timestamp, title, showPopover = true }) => {
    if (!timestamp) {
        return <InfoItemContent>-</InfoItemContent>;
    }

    const relativeTime = toRelativeTimeString(timestamp);
    const absoluteTime = toLocalDateTimeString(timestamp);

    if (!showPopover) {
        return <InfoItemContent>{relativeTime}</InfoItemContent>;
    }

    return (
        <Popover
            content={<PopoverContent>{absoluteTime}</PopoverContent>}
            title={<Title>{title}</Title>}
            trigger="hover"
            overlayInnerStyle={popoverStyles.overlayInnerStyle}
            overlayStyle={popoverStyles.overlayStyle}
        >
            <InfoItemContent>{relativeTime}</InfoItemContent>
        </Popover>
    );
};
