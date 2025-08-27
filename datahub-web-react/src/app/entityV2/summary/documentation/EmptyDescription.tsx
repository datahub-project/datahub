import { Text } from '@components';
import React from 'react';

import { useDescriptionUtils } from '@app/entityV2/summary/documentation/useDescriptionUtils';

export default function EmptyDescription() {
    const { emptyDescriptionText } = useDescriptionUtils();

    return (
        <Text color="gray" colorLevel={1800}>
            {emptyDescriptionText}
        </Text>
    );
}
