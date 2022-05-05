import React, { useState } from 'react';
import {
    CheckOutlined,
    CopyOutlined,
    ExclamationCircleOutlined,
    FolderOpenOutlined,
    InfoCircleOutlined,
    LinkOutlined,
    MoreOutlined,
    RightOutlined,
} from '@ant-design/icons';
import { Typography, Image, Button, Tooltip, Menu, Dropdown, message, Popover } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import moment from 'moment';

import { EntityType } from '../../../../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../../../../shared/textUtil';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../Entity';
import { ANTD_GRAY } from '../../../constants';
import { useEntityData, useRefetch } from '../../../EntityContext';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { EntityHealthStatus } from './EntityHealthStatus';
import { useUpdateDeprecationMutation } from '../../../../../../graphql/mutations.generated';
import { getLocaleTimezone } from '../../../../../shared/time/timeUtils';
import { AddDeprecationDetailsModal } from './AddDeprecationDetailsModal';

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
    flex-wrap: wrap;
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

const Divider = styled.div`
    border-top: 1px solid #f0f0f0;
    padding-top: 5px;
`;

const SideHeaderContent = styled.div`
    display: flex;
    flex-direction: column;
`;

const TopButtonsWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    margin-bottom: 8px;
`;

type Props = {
    showDeprecateOption?: boolean;
};

export const EntityHeader = ({ showDeprecateOption }: Props) => {
    const { urn, entityType, entityData } = useEntityData();
    const [updateDeprecation] = useUpdateDeprecationMutation();
    const [showAddDeprecationDetailsModal, setShowAddDeprecationDetailsModal] = useState(false);
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

    // Update the Deprecation
    const handleUpdateDeprecation = async (deprecatedStatus: boolean) => {
        message.loading({ content: 'Updating...' });
        try {
            await updateDeprecation({
                variables: {
                    input: {
                        urn,
                        deprecated: deprecatedStatus,
                        note: '',
                        decommissionTime: null,
                    },
                },
            });
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

    /**
     * Deprecation Decommission Timestamp
     */
    const localeTimezone = getLocaleTimezone();
    const decommissionTimeLocal =
        (entityData?.deprecation?.decommissionTime &&
            `Scheduled to be decommissioned on ${moment
                .unix(entityData?.deprecation?.decommissionTime)
                .format('DD/MMM/YYYY')} at ${moment
                .unix(entityData?.deprecation?.decommissionTime)
                .format('HH:mm:ss')} (${localeTimezone})`) ||
        undefined;
    const decommissionTimeGMT =
        entityData?.deprecation?.decommissionTime &&
        moment.unix(entityData?.deprecation?.decommissionTime).utc().format('dddd, DD/MMM/YYYY HH:mm:ss z');

    const hasDetails = entityData?.deprecation?.note !== '' || entityData?.deprecation?.decommissionTime !== null;
    const isDividerNeeded = entityData?.deprecation?.note !== '' && entityData?.deprecation?.decommissionTime !== null;
    const showAdditionalOptions = showDeprecateOption;
    const pageUrl = window.location.href;

    return (
        <>
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
                        <EntityTitle level={3}>{entityData?.name || ' '}</EntityTitle>
                        {entityData?.deprecation?.deprecated && (
                            <Popover
                                overlayStyle={{ maxWidth: 240 }}
                                placement="right"
                                content={
                                    hasDetails ? (
                                        <>
                                            {entityData?.deprecation?.note !== '' && (
                                                <Typography.Text>{entityData?.deprecation?.note}</Typography.Text>
                                            )}
                                            {isDividerNeeded && <Divider />}
                                            {entityData?.deprecation?.decommissionTime !== null && (
                                                <Typography.Text type="secondary">
                                                    <Tooltip placement="right" title={decommissionTimeGMT}>
                                                        <LastEvaluatedAtLabel>
                                                            {decommissionTimeLocal}
                                                        </LastEvaluatedAtLabel>
                                                    </Tooltip>
                                                </Typography.Text>
                                            )}
                                        </>
                                    ) : (
                                        'No additional details'
                                    )
                                }
                            >
                                <DeprecatedContainer>
                                    <InfoCircleOutlined />
                                    <DeprecatedText>Deprecated</DeprecatedText>
                                </DeprecatedContainer>
                            </Popover>
                        )}
                        {entityData?.health && (
                            <EntityHealthStatus
                                status={entityData?.health.status}
                                message={entityData?.health?.message || undefined}
                            />
                        )}
                    </div>
                </MainHeaderContent>
                <SideHeaderContent>
                    <TopButtonsWrapper>
                        <Tooltip title="Copy URN. An URN uniquely identifies an entity on DataHub.">
                            <Button
                                icon={copiedUrn ? <CheckOutlined /> : <CopyOutlined />}
                                onClick={() => {
                                    navigator.clipboard.writeText(urn);
                                    setCopiedUrn(true);
                                }}
                            />
                        </Tooltip>
                        {showAdditionalOptions && (
                            <Dropdown
                                overlay={
                                    <Menu>
                                        <Menu.Item key="0">
                                            <MenuItem
                                                onClick={() => {
                                                    navigator.clipboard.writeText(pageUrl);
                                                    message.info('Copied URL!', 1.2);
                                                }}
                                            >
                                                <LinkOutlined /> &nbsp; Copy Url
                                            </MenuItem>
                                        </Menu.Item>
                                        <Menu.Item key="1">
                                            {!entityData?.deprecation?.deprecated ? (
                                                <MenuItem onClick={() => setShowAddDeprecationDetailsModal(true)}>
                                                    <ExclamationCircleOutlined /> &nbsp; Mark as deprecated
                                                </MenuItem>
                                            ) : (
                                                <MenuItem onClick={() => handleUpdateDeprecation(false)}>
                                                    <ExclamationCircleOutlined /> &nbsp; Mark as un-deprecated
                                                </MenuItem>
                                            )}
                                        </Menu.Item>
                                    </Menu>
                                }
                                trigger={['click']}
                            >
                                <MenuIcon />
                            </Dropdown>
                        )}
                    </TopButtonsWrapper>
                    {hasExternalUrl && (
                        <Button href={externalUrl} onClick={sendAnalytics}>
                            View in {platformName}
                            <RightOutlined style={{ fontSize: 12 }} />
                        </Button>
                    )}
                </SideHeaderContent>
            </HeaderContainer>
            <AddDeprecationDetailsModal
                visible={showAddDeprecationDetailsModal}
                urn={urn}
                onClose={() => {
                    setShowAddDeprecationDetailsModal(false);
                }}
                refetch={refetch}
            />
        </>
    );
};
