import {
    formatDetailedDuration,
    formatDuration,
    toLocalDateTimeString,
    toRelativeTimeString,
} from '@app/shared/time/timeUtils';
import { Pill, Popover } from '@components';
import { capitalize } from 'lodash';
import React from 'react';
import styled from 'styled-components';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { DataProcessInstanceRunResultType } from '../../types.generated';

const StatContainer = styled.div`
    display: flex;
    margin-top: 40px;
    height: 25px;
    padding-left: 20px;
    color: ${colors.gray[500]};
    width: 150px;
`;

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

const popoverStyles = {
    overlayInnerStyle: {
        borderRadius: '10px',
    },
    overlayStyle: {
        margin: '5px',
    },
};

interface Props {
    startTime?: number;
    duration?: number;
    status?: string;
}

export default function DataProcessInstanceRightColumn({ startTime, duration, status }: Props) {
    const statusPillColor = status === DataProcessInstanceRunResultType.Success ? 'green' : 'red';

    return (
        <>
            {startTime && (
                <Popover
                    content={<PopoverContent>{toLocalDateTimeString(startTime)}</PopoverContent>}
                    title={<Title>Start Time</Title>}
                    trigger="hover"
                    overlayInnerStyle={popoverStyles.overlayInnerStyle}
                    overlayStyle={popoverStyles.overlayStyle}
                >
                    <StatContainer>{toRelativeTimeString(startTime)}</StatContainer>
                </Popover>
            )}
            {duration && (
                <Popover
                    content={<PopoverContent>{formatDetailedDuration(duration)}</PopoverContent>}
                    title={<Title>Duration</Title>}
                    trigger="hover"
                    overlayInnerStyle={popoverStyles.overlayInnerStyle}
                    overlayStyle={popoverStyles.overlayStyle}
                >
                    <StatContainer>{formatDuration(duration)}</StatContainer>
                </Popover>
            )}
            {status && (
                <>
                    <StatContainer>
                        <Pill label={capitalize(status)} color={statusPillColor} clickable={false} />
                    </StatContainer>
                </>
            )}
        </>
    );
}
