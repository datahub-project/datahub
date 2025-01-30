import { Typography } from 'antd';
import React from 'react';
import { CorpUser, EntityType } from '../../../types.generated';
import { CustomAvatar } from '../../shared/avatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getAutoCompleteEntityText } from './utils';
import { SuggestionText } from './styledComponents';

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
