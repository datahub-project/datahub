import { Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { IconStyleType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { SEPARATE_SIBLINGS_URL_PARAM } from '@app/entity/shared/siblingUtils';
import { getEntityNameAndLogo } from '@app/settingsV2/personal/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataHubSubscription, EntityType } from '@types';

const EntityColumnContainer = styled.div`
    margin-bottom: 16px;
`;

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

const EntityTypeContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 4px;
`;

const EntityTypeText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 400;
    color: ${ANTD_GRAY[8]};
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
    const { entity } = subscription;
    const entityRegistry = useEntityRegistry();
    const entityType: EntityType = entity.type;
    const entityUrn: string = entity.urn;
    const entityTypeDisplayName = entityRegistry.getEntityName(entityType);
    const entityName: string = entityRegistry.getDisplayName(entityType, entity);
    const entityTypeIcon = entityRegistry.getIcon(entityType, 14, IconStyleType.ACCENT);
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

    return (
        <EntityColumnContainer>
            <Link to={entityUrl}>
                <ContentContainer>
                    <PlatformTypeContainer>
                        <Tooltip overlay={platformTypeDisplayName}>{hasIcon ? platformIcon : defaultIcon}</Tooltip>
                    </PlatformTypeContainer>
                    <EntityNameContainer>
                        <EntityTypeContainer>
                            {entityTypeIcon}
                            <EntityTypeText>{entityTypeDisplayName}</EntityTypeText>
                        </EntityTypeContainer>
                        <EntityNameText>{entityName}</EntityNameText>
                    </EntityNameContainer>
                </ContentContainer>
            </Link>
        </EntityColumnContainer>
    );
}
