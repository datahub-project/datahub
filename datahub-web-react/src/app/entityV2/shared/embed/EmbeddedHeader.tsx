import { Image, Typography } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components/macro';
import Link from 'antd/lib/typography/Link';
import { ArrowRightOutlined } from '@ant-design/icons';
import { DEFAULT_APP_CONFIG } from '../../../../appConfigContext';
import { useAppConfig } from '../../../useAppConfig';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { getDisplayedEntityType } from '../containers/profile/header/utils';
import { ANTD_GRAY } from '../constants';
import analytics from '../../../analytics/analytics';
import { EventType } from '../../../analytics';

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
    color: ${ANTD_GRAY[8]};
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

    const typeIcon = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT, ANTD_GRAY[8]);
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
