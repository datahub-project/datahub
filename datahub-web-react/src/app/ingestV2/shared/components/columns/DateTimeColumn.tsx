import { Text } from '@components';
import dayjs from 'dayjs';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import isPresent from '@app/utils/isPresent';

/**
 * l - M/D/YYYY
 * LT - h:mm A
 */
const DEFAULT_DATETIME_FORMAT = 'l @ LT';

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
