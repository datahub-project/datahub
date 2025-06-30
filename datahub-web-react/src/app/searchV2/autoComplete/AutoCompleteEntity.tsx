import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { getPlatformName } from '@app/entity/shared/utils';
import AutoCompleteEntityIcon from '@app/searchV2/autoComplete/AutoCompleteEntityIcon';
import AutoCompletePlatformNames from '@app/searchV2/autoComplete/AutoCompletePlatformNames';
import { SuggestionText } from '@app/searchV2/autoComplete/styledComponents';
import { getAutoCompleteEntityText } from '@app/searchV2/autoComplete/utils';
import ParentEntities from '@app/searchV2/filters/ParentEntities';
import { getParentEntities } from '@app/searchV2/filters/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType } from '@types';

const AutoCompleteEntityWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
    align-items: center;
`;

const IconsContainer = styled.div`
    display: flex;
    gap: 4px;
`;

const ContentWrapper = styled.div`
    display: flex;
    align-items: center;
    overflow: hidden;
`;

const Subtype = styled.span`
    color: ${ANTD_GRAY_V2[8]};
    border: 1px solid ${ANTD_GRAY_V2[6]};
    border-radius: 16px;
    padding: 4px 8px;
    line-height: 12px;
    font-size: 12px;
    margin-right: 8px;
`;

const ItemHeader = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 3px;
    gap: 8px;
`;

const Divider = styled.div`
    border-right: 1px solid ${ANTD_GRAY_V2[6]};
    height: 12px;
`;

interface Props {
    query: string;
    entity: Entity;
    siblings?: Array<Entity>;
    hasParentTooltip: boolean;
}

// TODO: Migrate from using parent entities to using BrowsePathsV2 to mimic search card
export default function AutoCompleteEntity({ query, entity, siblings, hasParentTooltip }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const { matchedText, unmatchedText } = getAutoCompleteEntityText(displayName, query);
    const entities = siblings?.length ? siblings : [entity];
    const platformsToShow =
        /* Only show sibling platforms if there are > 0 explicitly included siblings */
        siblings?.length
            ? genericEntityProps?.siblingPlatforms
            : (genericEntityProps?.platform && [genericEntityProps?.platform]) || undefined;
    const platforms =
        platformsToShow
            ?.map(
                (platform) =>
                    getPlatformName(entityRegistry.getGenericEntityProperties(EntityType.DataPlatform, platform)) || '',
            )
            .filter(Boolean) ?? [];

    const parentContainers = genericEntityProps?.parentContainers?.containers || [];

    const subtype = genericEntityProps?.subTypes?.typeNames?.[0];

    // Parent entities are either a) containers or b) entity-type specific parents (glossary nodes, domains, etc)
    const parentEntities = (parentContainers?.length && parentContainers) || getParentEntities(entity) || [];

    const showPlatforms = !!platforms.length;
    const showPlatformDivider = !!platforms.length && !!parentContainers.length;
    const showParentEntities = !!parentEntities?.length;
    const showHeader = showPlatforms || showParentEntities;

    return (
        <AutoCompleteEntityWrapper data-testid={`auto-complete-entity-name-${displayName}`}>
            <ContentWrapper>
                <SuggestionText>
                    {showHeader && (
                        <ItemHeader>
                            <IconsContainer>
                                {entities.map((ent) => (
                                    <AutoCompleteEntityIcon key={ent.urn} entity={ent} />
                                ))}
                            </IconsContainer>
                            {showPlatforms && <AutoCompletePlatformNames platforms={platforms} />}
                            {showPlatformDivider && <Divider />}
                            {showParentEntities && <ParentEntities parentEntities={parentEntities} />}
                        </ItemHeader>
                    )}
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
