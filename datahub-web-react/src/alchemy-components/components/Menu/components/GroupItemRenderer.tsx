import { Text } from '@components';
import React from 'react';

import { GroupItemRendererProps } from '@components/components/Menu/types';

export default function GroupItemRenderer({ item }: GroupItemRendererProps) {
    return (
        <Text color="gray" weight="bold" size="sm">
            {item.title}
        </Text>
    );
}
