import React, { useState } from 'react';
import { CheckOutlined, CopyOutlined, FolderOpenOutlined, InfoCircleOutlined, MoreOutlined } from '@ant-design/icons';
import { Typography, Image, Button, Tooltip, Menu, Dropdown, message, Popover } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { EntityType } from '../../../../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../../../../shared/textUtil';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../Entity';
import { ANTD_GRAY } from '../../../constants';
import { useEntityData, useRefetch } from '../../../EntityContext';
import { useEntityPath } from '../utils';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { EntityHealthStatus } from './EntityHealthStatus';
import { useUpdateDeprecationMutation } from '../../../../../../graphql/mutations.generated';
import { getLocaleTimezone } from '../../../../../shared/time/timeUtils';

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

const ContainerText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 400;
    color: ${ANTD_GRAY[9]};
`;

const ContainerIcon = styled(FolderOpenOutlined)`
    &&& {
        font-size: 12px;
        margin-right: 4px;
    }
`;

const DeprecatedContainer = styled.div`
    width: 110px;
    height: 18px;
    border: 1px solid #ef5b5b;
    border-radius: 15px;
    display: flex;
    justify-content: center;
    align-items: center;
    color: #ef5b5b;
    margin-left: 15px;
    padding-top: 12px;
    padding-bottom: 12px;
`;

const DeprecatedText = styled.div`
    color: #ef5b5b;
    margin-left: 5px;
`;

const MenuIcon = styled(MoreOutlined)`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: 25px;
    height: 32px;
    margin-left: 5px;
`;

const MenuItem = styled.div`
    font-size: 12px;
    padding-left: 12px;
    padding-right: 12px;
    color: rgba(0, 0, 0, 0.85);
`;

const LastEvaluatedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[7]};
`;

export const EntityHeader = () => {
    const { urn, entityType, entityData } = useEntityData();
    const [updateDeprecation] = useUpdateDeprecationMutation();
    const refetch = useRefetch();
    const entityRegistry = useEntityRegistry();
    const [copiedUrn, setCopiedUrn] = useState(false);
    const basePlatformName = entityData?.platform?.properties?.displayName || entityData?.platform?.name;
    const platformName = capitalizeFirstLetterOnly(basePlatformName);
    const platformLogoUrl = entityData?.platform?.properties?.logoUrl;
    const entityLogoComponent = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const entityTypeCased =
        (entityData?.subTypes?.typeNames?.length && capitalizeFirstLetterOnly(entityData?.subTypes.typeNames[0])) ||
        entityRegistry.getEntityName(entityType);
    const entityPath = useEntityPath(entityType, urn);
    const externalUrl = entityData?.externalUrl || undefined;
    const hasExternalUrl = !!externalUrl;

    const sendAnalytics = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType,
            entityUrn: urn,
        });
    };

    const entityCount = entityData?.entityCount;
    const typeIcon = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const container = entityData?.container;

    // Update Deprecation
    const UpdateDeprecationMutation = (deprecatedStatus: boolean) => {
        return updateDeprecation({
            variables: {
                input: {
                    urn,
                    deprecated: deprecatedStatus,
                },
            },
        });
    };

    // Update the Deprecation
    const handleUpdateDeprecation = async (deprecatedStatus: boolean) => {
        message.loading({ content: 'Updating...' });
        try {
            await UpdateDeprecationMutation(deprecatedStatus);
            message.destroy();
            message.success({ content: 'Deprecation Updated', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update Deprecation: \n ${e.message || ''}`, duration: 2 });
            }
        }
        refetch?.();
    };

    const menu = (
        <Menu>
            <Menu.Item key="0">
                {!entityData?.deprecation?.deprecated ? (
                    <MenuItem onClick={() => handleUpdateDeprecation(true)}>Mark as deprecated</MenuItem>
                ) : (
                    <MenuItem onClick={() => handleUpdateDeprecation(false)}>Mark as un-deprecated</MenuItem>
                )}
            </Menu.Item>
        </Menu>
    );

    /**
     * Deprecation Decommission Timestamp
     */
    // const lastEvaluatedAt = new Date(entityData?.deprecation?.decommissionTime);
    const localeTimezone = getLocaleTimezone();
    const lastEvaluatedAt = new Date();
    const lastEvaluatedTimeLocal =
        (lastEvaluatedAt &&
            `Last evaluated on ${lastEvaluatedAt.toLocaleDateString()} at ${lastEvaluatedAt.toLocaleTimeString()} (${localeTimezone})`) ||
        'No evaluations found';
    const lastEvaluatedTimeGMT = lastEvaluatedAt && lastEvaluatedAt.toUTCString();

    return (
        <HeaderContainer>
            <MainHeaderContent>
                <PlatformContent>
                    {platformName && (
                        <LogoContainer>
                            {(!!platformLogoUrl && (
                                <PreviewImage preview={false} src={platformLogoUrl} alt={platformName} />
                            )) ||
                                entityLogoComponent}
                        </LogoContainer>
                    )}
                    <PlatformText>{platformName}</PlatformText>
                    {(platformLogoUrl || platformName) && <PlatformDivider />}
                    {typeIcon && <TypeIcon>{typeIcon}</TypeIcon>}
                    <PlatformText>{entityData?.entityTypeOverride || entityTypeCased}</PlatformText>
                    {container && (
                        <Link to={entityRegistry.getEntityUrl(EntityType.Container, container?.urn)}>
                            <PlatformDivider />
                            <ContainerIcon
                                style={{
                                    color: ANTD_GRAY[9],
                                }}
                            />
                            <ContainerText>
                                {entityRegistry.getDisplayName(EntityType.Container, container)}
                            </ContainerText>
                        </Link>
                    )}
                    {entityCount && entityCount > 0 ? (
                        <>
                            <PlatformDivider />
                            <EntityCountText>{entityCount.toLocaleString()} entities</EntityCountText>
                        </>
                    ) : null}
                </PlatformContent>
                <div style={{ display: 'flex', justifyContent: 'left', alignItems: 'center' }}>
                    <Link to={entityPath}>
                        <EntityTitle level={3}>{entityData?.name || ' '}</EntityTitle>
                    </Link>
                    <Popover
                        overlayStyle={{ maxWidth: 600 }}
                        placement="right"
                        title={<Typography.Text strong>This entity is deprecated.</Typography.Text>}
                        content={
                            <Typography.Text type="secondary">
                                {entityData?.deprecation?.decommissionTime === null ? (
                                    <Tooltip placement="right" title={lastEvaluatedTimeGMT}>
                                        <LastEvaluatedAtLabel>
                                            Scheduled to be decommissioned at {lastEvaluatedTimeLocal}
                                        </LastEvaluatedAtLabel>
                                    </Tooltip>
                                ) : (
                                    'No Decommission Time'
                                )}
                            </Typography.Text>
                        }
                    >
                        {entityData?.deprecation?.deprecated && (
                            <DeprecatedContainer>
                                <InfoCircleOutlined />
                                <DeprecatedText>Deprecated</DeprecatedText>
                            </DeprecatedContainer>
                        )}
                    </Popover>

                    {entityData?.health && (
                        <EntityHealthStatus
                            status={entityData?.health.status}
                            message={entityData?.health?.message || undefined}
                        />
                    )}
                </div>
            </MainHeaderContent>
            {hasExternalUrl && (
                <ExternalLinkButton href={externalUrl} onClick={sendAnalytics}>
                    View in {platformName}
                </ExternalLinkButton>
            )}
            <Tooltip title="Copy URN. An URN uniquely identifies an entity on DataHub.">
                <Button
                    icon={copiedUrn ? <CheckOutlined /> : <CopyOutlined />}
                    onClick={() => {
                        navigator.clipboard.writeText(urn);
                        setCopiedUrn(true);
                    }}
                />
            </Tooltip>
            <Dropdown overlay={menu} trigger={['click']}>
                <MenuIcon />
            </Dropdown>
        </HeaderContainer>
    );
};
