import React from 'react';
import { Text } from '@src/alchemy-components';
import { isItCorpUserEntity } from '@src/app/entityV2/user/utils';
import { EntitySubtitleProps } from './types';

export default function UserEntitySubtitle({ entity }: EntitySubtitleProps) {
    if (!isItCorpUserEntity(entity)) return null;

    const userName = entity?.username;

    return (
        <Text color="gray" size="sm">
            {userName}
        </Text>
    );
}
