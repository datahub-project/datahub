import React from 'react';

import { Text } from '@src/alchemy-components';

interface Props {
    text: string;
}

export default function SectionHeader({ text }: Props) {
    return <Text weight="semiBold">{text}</Text>;
}
