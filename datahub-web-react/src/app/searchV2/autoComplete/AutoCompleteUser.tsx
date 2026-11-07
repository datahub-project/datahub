import { Avatar, Text } from '@components';
import React from 'react';

import { AvatarType } from '@components/components/AvatarStack/types';

import { SuggestionText } from '@app/searchV2/autoComplete/styledComponents';
import { getAutoCompleteEntityText } from '@app/searchV2/autoComplete/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, EntityType } from '@types';

interface Props {
    query: string;
    user: CorpUser;
}

export default function AutoCompleteUser({ query, user }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpUser, user);
    const { matchedText, unmatchedText } = getAutoCompleteEntityText(displayName, query);

    return (
        <>
            <Avatar
                name={displayName}
                imageUrl={user.editableProperties?.pictureLink || undefined}
                type={AvatarType.user}
                size="sm"
            />
            <SuggestionText>
                <Text type="span" size="inherit" weight="semiBold">
                    {matchedText}
                </Text>
                {unmatchedText}
            </SuggestionText>
        </>
    );
}
