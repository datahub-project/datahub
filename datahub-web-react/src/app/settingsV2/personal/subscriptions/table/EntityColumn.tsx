import { Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { IconStyleType } from '@app/entity/Entity';
import { SEPARATE_SIBLINGS_URL_PARAM } from '@app/entity/shared/siblingUtils';
import ContextPath from '@app/previewV2/ContextPath';
import { getParentEntities } from '@app/searchV2/filters/utils';
import { getEntityNameAndLogo } from '@app/settingsV2/personal/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { BrowsePathV2, DataHubSubscription, EntityType, Maybe } from '@types';

const ContentContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: flex-start;
    align-items: center;
    gap: 16px;
`;

const PlatformTypeContainer = styled.div`
    max-width: 28px;
    max-height: 28px;
    width: auto;
    height: auto;
`;

const EntityNameContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const EntityNameText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 500;
`;

interface Props {
    subscription: DataHubSubscription;
}

export function EntityColumn({ subscription }: Props) {
    const { entity, urn } = subscription;
    const entityRegistry = useEntityRegistry();
    const entityType: EntityType = entity.type;
    const entityUrn: string = entity.urn;
    const entityName: string = entityRegistry.getDisplayName(entityType, entity);
    const entityUrl = `${entityRegistry.getEntityUrl(entityType, entityUrn)}?${SEPARATE_SIBLINGS_URL_PARAM}=true`;
    const { label: platformTypeDisplayName, icon: platformIcon } = getEntityNameAndLogo(
        entity,
        entityType,
        entityRegistry,
    );
    const hasIcon = platformIcon?.props?.src?.length;
    let defaultIcon: JSX.Element | null = null;
    if (!hasIcon) {
        defaultIcon = entityRegistry.getIcon(entityType, 28, IconStyleType.HIGHLIGHT);
    }
    const browsePath: Maybe<BrowsePathV2> | undefined =
        'browsePathV2' in entity ? (entity.browsePathV2 as Maybe<BrowsePathV2> | undefined) : undefined;

    const contextPath = getParentEntities(entity);
    return (
        <ContentContainer>
            <PlatformTypeContainer>
                <Tooltip overlay={platformTypeDisplayName}>{hasIcon ? platformIcon : defaultIcon}</Tooltip>
            </PlatformTypeContainer>
            <EntityNameContainer>
                <Link
                    to={entityUrl}
                    onClick={() => {
                        analytics.event({
                            type: EventType.SubscriptionEntityClickEvent,
                            subscriptionUrn: urn,
                            entityType,
                            entityUrn,
                            entityName,
                        });
                    }}
                >
                    <EntityNameText>{entityName}</EntityNameText>
                </Link>
                <ContextPath
                    entityType={entityType}
                    parentEntities={contextPath}
                    browsePaths={browsePath}
                    entityTitleWidth={150}
                    showPlatformText={false}
                    isCompactView
                    hideTypeIcons
                />
            </EntityNameContainer>
        </ContentContainer>
    );
}
