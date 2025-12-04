import { Text } from '@components';
import React from 'react';

interface Props {
    text: string;
}

export function HelperText({ text }: Props) {
    return (
        <Text size="sm" color="gray" colorLevel={1800}>
            {text}
        </Text>
    );
}
