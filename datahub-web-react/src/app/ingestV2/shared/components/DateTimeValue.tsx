import { Text } from '@components';
import dayjs from 'dayjs';
import React, { useMemo } from 'react';
import styled from 'styled-components';

/**
 * l - M/D/YYYY
 * LT - h:mm A
 */
const DEFAULT_DATETIME_FORMAT = 'l @ LT';

const StyledText = styled(Text)`
    text-wrap: auto;
`

interface Props {
    time?: number;
    format?: string;
    placeholder?: React.ReactElement;
}

export default function DateTimeValue({ time, format = DEFAULT_DATETIME_FORMAT, placeholder }: Props) {
    const formattedDateTime = useMemo(() => {
        if (!time) return undefined;
        return dayjs(time).format(format);
    }, [time]);

    if (!formattedDateTime) return placeholder || <></>;

    return <StyledText>{formattedDateTime}</StyledText>;
}
