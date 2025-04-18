import React from 'react';
<<<<<<< HEAD

import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import { Avatar } from '@src/alchemy-components';
import { isCorpUser } from '@src/app/entityV2/user/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
=======
import { Avatar } from '@src/alchemy-components';
import { isCorpUser } from '@src/app/entityV2/user/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { EntityIconProps } from './types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export default function UserEntityIcon({ entity }: EntityIconProps) {
    const entityRegistry = useEntityRegistryV2();

    if (!isCorpUser(entity)) return null;

    const imageUrl = entity?.editableProperties?.pictureLink;
    const displayName = entityRegistry.getDisplayName(entity.type, entity);

    return <Avatar name={displayName} imageUrl={imageUrl} size="md" />;
}
