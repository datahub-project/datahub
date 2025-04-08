import { Typography } from 'antd';
import React from 'react';

import { SuggestionText } from '@app/search/autoComplete/styledComponents';
import { getAutoCompleteEntityText } from '@app/search/autoComplete/utils';
import { CustomAvatar } from '@app/shared/avatar';
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
            <CustomAvatar
                size={20}
                name={displayName}
                photoUrl={user.editableProperties?.pictureLink || undefined}
                useDefaultAvatar={false}
                style={{
                    marginRight: 0,
                }}
            />
            <SuggestionText>
                <Typography.Text strong>{matchedText}</Typography.Text>
                {unmatchedText}
            </SuggestionText>
        </>
    );
}
