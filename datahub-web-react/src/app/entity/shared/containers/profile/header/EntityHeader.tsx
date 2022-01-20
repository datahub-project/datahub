import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { Typography, Image, Button, Tooltip } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { capitalizeFirstLetter } from '../../../../../shared/textUtil';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../Entity';
import { ANTD_GRAY } from '../../../constants';
import { useEntityData } from '../../../EntityContext';
import { useEntityPath } from '../utils';

const LogoContainer = styled.span`
    margin-right: 10px;
`;

const PreviewImage = styled(Image)`
    max-height: 17px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const EntityTitle = styled(Typography.Title)`
    &&& {
        margin-bottom: 0;
        word-break: break-all;
    }
`;

const PlatformContent = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 8px;
`;

const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 700;
    color: ${ANTD_GRAY[7]};
`;

const EntityCountText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 400;
    color: ${ANTD_GRAY[7]};
`;

const PlatformDivider = styled.div`
    display: inline-block;
    padding-left: 10px;
    margin-right: 10px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    height: 18px;
    vertical-align: text-top;
`;

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: space-between;
    margin-bottom: 4px;
`;

const MainHeaderContent = styled.div`
    flex: 1;
`;

const ExternalLinkButton = styled(Button)`
    margin-right: 12px;
`;

const TypeIcon = styled.span`
    margin-right: 8px;
`;

export const EntityHeader = () => {
    const { urn, entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const [copiedUrn, setCopiedUrn] = useState(false);
    const basePlatformName = entityData?.platform?.displayName || entityData?.platform?.name;
    const platformName = capitalizeFirstLetterOnly(basePlatformName);
    const platformLogoUrl = entityData?.platform?.properties?.logoUrl;
    const entityLogoComponent = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const entityTypeCased =
        (entityData?.subTypes?.typeNames?.length && capitalizeFirstLetterOnly(entityData?.subTypes.typeNames[0])) ||
        entityRegistry.getEntityName(entityType);
    const entityPath = useEntityPath(entityType, urn);
    const externalUrl = entityData?.externalUrl || undefined;
    const hasExternalUrl = !!externalUrl;
    const entityCount = entityData?.entityCount;
    const typeIcon = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    return (
        <HeaderContainer>
            <MainHeaderContent>
                <PlatformContent>
                    <LogoContainer>
                        {(!!platformLogoUrl && (
                            <PreviewImage preview={false} src={platformLogoUrl} alt={platformName} />
                        )) ||
                            entityLogoComponent}
                    </LogoContainer>
                    <PlatformText>{platformName}</PlatformText>
                    {(platformLogoUrl || platformName) && <PlatformDivider />}
                    {typeIcon && <TypeIcon>{typeIcon}</TypeIcon>}
                    <PlatformText>{entityData?.entityTypeOverride || entityTypeCased}</PlatformText>
                    {entityCount && (
                        <>
                            <PlatformDivider />
                            <EntityCountText>{entityCount.toLocaleString()} entities</EntityCountText>
                        </>
                    )}
                </PlatformContent>
                <Link to={entityPath}>
                    <EntityTitle level={3}>{entityData?.name || ' '}</EntityTitle>
                </Link>
            </MainHeaderContent>
            {hasExternalUrl && <ExternalLinkButton href={externalUrl}>View in {platformName}</ExternalLinkButton>}
            <Tooltip title="Copy URN. An URN uniquely identifies an entity on DataHub.">
                <Button
                    icon={copiedUrn ? <CheckOutlined /> : <CopyOutlined />}
                    onClick={() => {
                        navigator.clipboard.writeText(urn);
                        setCopiedUrn(true);
                    }}
                />
            </Tooltip>
        </HeaderContainer>
    );
};
