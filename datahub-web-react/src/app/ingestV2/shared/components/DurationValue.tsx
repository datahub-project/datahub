import { Text } from '@components';
import { useMemo } from 'react';

import { formatDuration } from '@app/shared/formatDuration';

interface Props {
    durationMs?: number;
    placeholder?: React.ReactElement;
    className?: string;
}

export default function DurationValue({ durationMs, placeholder, className }: Props) {
    const duration = useMemo(() => {
        if (durationMs === undefined) return undefined;
        return formatDuration(durationMs);
    }, [durationMs]);

    if (!duration) return placeholder ?? <></>;

    return <Text className={className}>{duration}</Text>;
}
