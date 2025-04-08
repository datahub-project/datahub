import React from 'react';
import { Text } from '@src/alchemy-components';
import { isCorpUser } from '@src/app/entityV2/user/utils';
import { EntitySubtitleProps } from './types';

export default function UserEntitySubtitle({ entity }: EntitySubtitleProps) {
    if (!isCorpUser(entity)) return null;

    const userName = entity?.username;

    return (
        <Text color="gray" size="sm">
            {userName}
        </Text>
    );
}
