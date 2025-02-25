import React from 'react';
import { CorpUser, EntityType } from '@src/types.generated';
import { Text } from '@src/alchemy-components';
import useEntityUtils from '@src/app/entityV2/shared/hooks/useEntityUtils';
import { EntitySubtitleProps } from './types';

export default function UserEntitySubtitle({ entity }: EntitySubtitleProps) {
    const { getUserName } = useEntityUtils();

    if (entity.type !== EntityType.CorpUser) return null;

    const userName = getUserName(entity as CorpUser);

    return (
        <Text color="gray" size="sm">
            @{userName}
        </Text>
    );
}
