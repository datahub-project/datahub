import { Text } from '@components';
import React from 'react';

interface Props {
    text: string | React.ReactNode;
}

export function HelperText({ text }: Props) {
    return (
        <Text size="sm" color="textTertiary">
            {text}
        </Text>
    );
}
