import { Image, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getPlatformName } from '../../entity/shared/utils';
import { IconStyleType } from '../../entity/Entity';
import { getAutoCompleteEntityText } from './utils';
import { SuggestionText } from './AutoCompleteUser';
import ParentContainers from './ParentContainers';
import { ANTD_GRAY } from '../../entity/shared/constants';

const AutoCompleteEntityWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
    align-items: center;
`;

const PreviewImage = styled(Image)`
    height: 22px;
    width: 22px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const ContentWrapper = styled.div`
    display: flex;
    align-items: center;
    overflow: hidden;
`;

const Subtype = styled.span`
    color: ${ANTD_GRAY[9]};
    border: 1px solid ${ANTD_GRAY[9]};
    border-radius: 16px;
    padding: 4px 8px;
    line-height: 12px;
    font-size: 12px;
    margin-right: 8px;
`;

interface Props {
    query: string;
    entity: Entity;
    hasParentTooltip: boolean;
}

export default function AutoCompleteEntity({ query, entity, hasParentTooltip }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformName = getPlatformName(genericEntityProps);
    const platformLogoUrl = genericEntityProps?.platform?.properties?.logoUrl;
    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const icon =
        (platformLogoUrl && <PreviewImage preview={false} src={platformLogoUrl} alt={platformName || ''} />) ||
        entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT);
    const { matchedText, unmatchedText } = getAutoCompleteEntityText(displayName, query);
    const parentContainers = genericEntityProps?.parentContainers?.containers || [];
    // Need to reverse parentContainers since it returns direct parent first.
    const orderedParentContainers = [...parentContainers].reverse();
    const subtype = genericEntityProps?.subTypes?.typeNames?.[0];

    return (
        <AutoCompleteEntityWrapper>
            <ContentWrapper>
                {icon}
                <SuggestionText>
                    <ParentContainers parentContainers={orderedParentContainers} />
                    <Typography.Text
                        ellipsis={
                            hasParentTooltip ? {} : { tooltip: { title: displayName, color: 'rgba(0, 0, 0, 0.9)' } }
                        }
                    >
                        <Typography.Text strong>{matchedText}</Typography.Text>
                        {unmatchedText}
                    </Typography.Text>
                </SuggestionText>
            </ContentWrapper>
            {subtype && <Subtype>{subtype.toLocaleLowerCase()}</Subtype>}
        </AutoCompleteEntityWrapper>
    );
}
