import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { CorpUser, EntityType } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { CustomAvatar } from '../../shared/avatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getAutoCompleteEntityText } from './utils';

export const SuggestionText = styled.div`
    margin-left: 12px;
    margin-top: 2px;
    margin-bottom: 2px;
    color: ${ANTD_GRAY[9]};
    font-size: 16px;
    overflow: hidden;
`;

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
