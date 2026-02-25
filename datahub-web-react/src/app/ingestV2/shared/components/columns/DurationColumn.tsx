import { Text } from '@components';
import React, { useMemo } from 'react';

import { formatDuration } from '@app/shared/formatDuration';
import isPresent from '@app/utils/isPresent';

interface Props {
    durationMs?: number | null;
    placeholder?: React.ReactElement;
    className?: string;
}

export default function DurationColumn({ durationMs, placeholder, className }: Props) {
    const duration = useMemo(() => {
        if (!isPresent(durationMs)) return undefined;
        return formatDuration(durationMs);
    }, [durationMs]);

    if (!duration) return placeholder ?? <>-</>;

    return <Text className={className}>{duration}</Text>;
}
