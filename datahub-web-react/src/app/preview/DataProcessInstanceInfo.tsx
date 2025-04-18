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
import { DataProcessInstanceRunResultType, DataProcessRunEvent } from '../../types.generated';

const StatContainer = styled.div`
    display: flex;
    margin-top: 20px;
    height: 20px;
    color: ${colors.gray[500]};
    width: 130px;
    justify-content: center;
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

export default function DataProcessInstanceInfo(lastRunEvent: DataProcessRunEvent) {
    const statusPillColor =
        lastRunEvent.result?.resultType === DataProcessInstanceRunResultType.Success ? 'green' : 'red';
    const startTime = lastRunEvent.timestampMillis ?? 0;
    const duration = lastRunEvent.durationMillis;
    const status = lastRunEvent.result?.resultType;

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
