import React from 'react';

import { GroupItemRendererProps } from '@components/components/Menu/types';
import { Text } from '@components/components/Text';

export default function GroupItemRenderer({ item }: GroupItemRendererProps) {
    return (
        <Text weight="bold" size="sm">
            {item.title}
        </Text>
    );
}
