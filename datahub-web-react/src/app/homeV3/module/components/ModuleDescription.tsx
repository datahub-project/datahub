import { Text } from '@components';
import React from 'react';

interface Props {
    text?: string;
}

export default function ModuleDescription({ text }: Props) {
    if (!text) return null;
    return (
        <Text color="gray" colorLevel={1700} size="md" weight="medium" lineHeight="xs">
            {text}
        </Text>
    );
}
