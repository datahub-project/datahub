import { Text } from '@components';
import dayjs from 'dayjs';
import advancedFormat from 'dayjs/plugin/advancedFormat';
import localizedFormat from 'dayjs/plugin/localizedFormat';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import isPresent from '@app/utils/isPresent';

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(advancedFormat);
dayjs.extend(localizedFormat);

/**
 * l - M/D/YYYY
 * LT - h:mm A
 * * z - timezone shorthand
 */
const DEFAULT_DATETIME_FORMAT = 'l @ LT (z)';

const StyledText = styled(Text)`
    text-wrap: auto;
`;

interface Props {
    time?: number | null;
    format?: string;
    placeholder?: React.ReactElement;
}

export default function DateTimeColumn({ time, format = DEFAULT_DATETIME_FORMAT, placeholder }: Props) {
    const formattedDateTime = useMemo(() => {
        if (!isPresent(time)) return undefined;
        return dayjs(time).format(format);
    }, [time, format]);

    if (!formattedDateTime) return placeholder || <>-</>;

    return <StyledText>{formattedDateTime}</StyledText>;
}
