import { Text } from '@components';
import React from 'react';

interface Props {
    text?: string;
}

export default function ModuleName({ text }: Props) {
    return (
        <Text color="gray" colorLevel={600} size="xl" weight="bold" lineHeight="sm">
            {text}
        </Text>
    );
}
