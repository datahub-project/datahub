/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
