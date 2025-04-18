import React from 'react';
<<<<<<< HEAD

=======
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
import { Text } from '@src/alchemy-components';

interface Props {
    text: string;
}

export default function SectionHeader({ text }: Props) {
    return (
        <Text color="gray" weight="semiBold">
            {text}
        </Text>
    );
}
