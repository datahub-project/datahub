import { Image, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getPlatformName } from '../../entity/shared/utils';
import { IconStyleType } from '../../entity/Entity';
import { getAutoCompleteEntityText } from './utils';
import { SuggestionText } from './AutoCompleteUser';

const PreviewImage = styled(Image)`
    max-height: 16px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

interface Props {
    query: string;
    entity: Entity;
}

export default function AutoCompleteEntity({ query, entity }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformName = getPlatformName(genericEntityProps);
    const platformLogoUrl = genericEntityProps?.platform?.properties?.logoUrl;
    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const icon =
        (platformLogoUrl && <PreviewImage preview={false} src={platformLogoUrl} alt={platformName || ''} />) ||
        entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT);
    const { matchedText, unmatchedText } = getAutoCompleteEntityText(displayName, query);

    return (
        <>
            {icon}
            <SuggestionText>
                <Typography.Text strong>{matchedText}</Typography.Text>
                {unmatchedText}
            </SuggestionText>
        </>
    );
}
