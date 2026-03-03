import { ArrowRightOutlined } from '@ant-design/icons';
import { Image, Typography } from 'antd';
import Link from 'antd/lib/typography/Link';
import React from 'react';
import styled, { useTheme } from 'styled-components/macro';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { IconStyleType } from '@app/entityV2/Entity';
import { getDisplayedEntityType } from '@app/entityV2/shared/containers/profile/header/utils';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { DEFAULT_APP_CONFIG } from '@src/appConfigContext';

const HeaderWrapper = styled.div`
    display: flex;
`;

const LogoImage = styled(Image)`
    display: inline-block;
    height: 40px;
    width: auto;
`;

const EntityContent = styled.div`
    margin-left: 16px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const EntityTypeWrapper = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const TypeIcon = styled.span`
    margin-right: 5px;
`;

const EntityName = styled(Typography.Text)`
    font-size: 16px;
    font-weight: 700;
    line-height: 24px;
`;

const StyledLink = styled(Link)`
    font-size: 10px;
    font-weight: 700;
    line-height: 22px;
    margin-left: 8px;
`;

const EntityNameWrapper = styled.div`
    display: flex;
    align-items: baseline;
`;

export default function EmbeddedHeader() {
    const entityRegistry = useEntityRegistry();
    const { entityData, entityType } = useEntityData();
    const appConfig = useAppConfig();
    const themeConfig = useTheme();

    function trackClickViewInDataHub() {
        analytics.event({
            type: EventType.EmbedProfileViewInDataHubEvent,
            entityType,
            entityUrn: entityData?.urn || '',
        });
    }

    const typeIcon = entityRegistry.getIcon(entityType, 14, IconStyleType.ACCENT);
    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);
    const entityName = entityRegistry.getDisplayName(entityType, entityData);
    const entityTypePathName = entityRegistry.getPathName(entityType);
    const logoUrl =
        appConfig.config !== DEFAULT_APP_CONFIG
            ? appConfig.config.visualConfig.logoUrl || themeConfig.assets.logoUrl
            : undefined;

    return (
        <HeaderWrapper>
            <LogoImage src={logoUrl} preview={false} />
            <EntityContent>
                <EntityTypeWrapper>
                    <TypeIcon>{typeIcon}</TypeIcon>
                    {displayedEntityType}
                </EntityTypeWrapper>
                <EntityNameWrapper>
                    <EntityName ellipsis={{ tooltip: entityName }} style={{ maxWidth: '75%' }}>
                        {entityName}
                    </EntityName>
                    <StyledLink
                        href={`${window.location.origin}/${entityTypePathName}/${entityData?.urn}`}
                        target="_blank"
                        rel="noreferrer noopener"
                        onClick={trackClickViewInDataHub}
                    >
                        view in DataHub <ArrowRightOutlined />
                    </StyledLink>
                </EntityNameWrapper>
            </EntityContent>
        </HeaderWrapper>
    );
}
