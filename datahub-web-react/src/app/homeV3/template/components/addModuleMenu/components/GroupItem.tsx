import { Text } from '@components';
import React from 'react';

interface Props {
    title: string;
}

export default function GroupItem({ title }: Props) {
    return (
        <Text color="gray" weight="bold">
            {title}
        </Text>
    );
}
