import { CellHoverWrapper, Text, Tooltip } from '@components';
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

const StyledText = styled(Text)`
    text-wrap: auto;
    width: max-content;
`;

interface Props {
    time?: number | null;
    format?: string;
    placeholder?: React.ReactElement;
    showRelative?: boolean;
}

export default function DateTimeColumn({ time, format = DEFAULT_DATETIME_FORMAT, placeholder, showRelative }: Props) {
    const formattedDateTime = useMemo(() => {
        if (!isPresent(time) || time === 0) return undefined;
        return dayjs(time).format(format);
    }, [time, format]);

    if (!isPresent(time) || time === 0) return placeholder || <>-</>;

    const relativeTime = toRelativeTimeString(time);

    return (
        <>
            {showRelative ? (
                <StyledText data-testid="ingestion-source-last-run">{relativeTime}</StyledText>
            ) : (
                <StyledText>{formattedDateTime}</StyledText>
            )}
        </>
    );
}

const DateTimeCellWrapper = styled(CellHoverWrapper)`
    .hoverable-cell:hover ${StyledText} {
        text-decoration: underline;
    }
`;

export function wrapDateTimeColumnWithHover(
    content: React.ReactNode,
    time: number | null | undefined,
): React.ReactNode {
    if (!isPresent(time) || time === 0) {
        return content;
    }

    const formattedDateTime = dayjs(time).format(DEFAULT_DATETIME_FORMAT);

    return (
        <Tooltip placement="topLeft" title={formattedDateTime}>
            <DateTimeCellWrapper>{content}</DateTimeCellWrapper>
        </Tooltip>
    );
}

export function formatDateTime(time?: number) {
    if (!isPresent(time) || time === 0) {
        return undefined;
    }
    return dayjs(time).format(DEFAULT_DATETIME_FORMAT);
}
