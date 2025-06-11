import { Text, Tooltip } from '@components';
import dayjs from 'dayjs';
import advancedFormat from 'dayjs/plugin/advancedFormat';
import localizedFormat from 'dayjs/plugin/localizedFormat';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import isPresent from '@app/utils/isPresent';

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(advancedFormat);
dayjs.extend(localizedFormat);

/**
 * l - M/D/YYYY
 * LT - h:mm A
 * z - timezone shorthand
 */
const DEFAULT_DATETIME_FORMAT = 'l @ LT (z)';

const StyledText = styled(Text)<{ $shouldUnderline?: boolean }>`
    text-wrap: auto;

    ${(props) =>
        props.$shouldUnderline &&
        `
            :hover {
                text-decoration: underline;
            }
        
        `}
`;

interface Props {
    time?: number | null;
    format?: string;
    placeholder?: React.ReactElement;
    showRelative?: boolean;
    onClick?: () => void;
}

export default function DateTimeColumn({
    time,
    format = DEFAULT_DATETIME_FORMAT,
    placeholder,
    showRelative,
    onClick,
}: Props) {
    const formattedDateTime = useMemo(() => {
        if (!isPresent(time)) return undefined;
        return dayjs(time).format(format);
    }, [time, format]);

    if (!isPresent(time) || time === 0) return placeholder || <>-</>;

    const relativeTime = toRelativeTimeString(time);

    return (
        <>
            {showRelative ? (
                <Tooltip title={formattedDateTime}>
                    <StyledText
                        onClick={(e) => {
                            e.stopPropagation();
                            onClick?.();
                        }}
                        $shouldUnderline={!!onClick}
                    >
                        {relativeTime}
                    </StyledText>
                </Tooltip>
            ) : (
                <StyledText>{formattedDateTime}</StyledText>
            )}
        </>
    );
}
