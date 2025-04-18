import React from 'react';
<<<<<<< HEAD

import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
import { Text } from '@src/alchemy-components';
import { isCorpUser } from '@src/app/entityV2/user/utils';
=======
import { Text } from '@src/alchemy-components';
import { isCorpUser } from '@src/app/entityV2/user/utils';
import { EntitySubtitleProps } from './types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export default function UserEntitySubtitle({ entity }: EntitySubtitleProps) {
    if (!isCorpUser(entity)) return null;

    const userName = entity.username;

    return (
        <Text color="gray" size="sm">
            {userName}
        </Text>
    );
}
