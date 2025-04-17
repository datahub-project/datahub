import React from 'react';

import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
import { Text } from '@src/alchemy-components';
import { isCorpUser } from '@src/app/entityV2/user/utils';

export default function UserEntitySubtitle({ entity }: EntitySubtitleProps) {
    if (!isCorpUser(entity)) return null;

    const userName = entity.username;

    return (
        <Text color="gray" size="sm">
            {userName}
        </Text>
    );
}
