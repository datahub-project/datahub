/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import { Avatar } from '@src/alchemy-components';
import { isCorpUser } from '@src/app/entityV2/user/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

export default function UserEntityIcon({ entity }: EntityIconProps) {
    const entityRegistry = useEntityRegistryV2();

    if (!isCorpUser(entity)) return null;

    const imageUrl = entity?.editableProperties?.pictureLink;
    const displayName = entityRegistry.getDisplayName(entity.type, entity);

    return <Avatar name={displayName} imageUrl={imageUrl} size="md" />;
}
