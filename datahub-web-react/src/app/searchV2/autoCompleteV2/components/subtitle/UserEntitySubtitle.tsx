/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
import { Text } from '@src/alchemy-components';
import { isCorpUser } from '@src/app/entityV2/user/utils';

export default function UserEntitySubtitle({ entity, color, colorLevel }: EntitySubtitleProps) {
    if (!isCorpUser(entity)) return null;

    const userName = entity.username;

    return (
        <Text color={color} colorLevel={colorLevel} size="sm">
            {userName}
        </Text>
    );
}
