import {
    DeleteOutlined,
    EditOutlined,
    FolderAddOutlined,
    FolderOpenOutlined,
    LinkOutlined,
    NotificationOutlined,
    PlusOutlined,
    ShareAltOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import { Tooltip } from '@components';
import MoreVertOutlinedIcon from '@mui/icons-material/MoreVertOutlined';
import { Dropdown, Menu, message } from 'antd';
import { GitCommit, LinkBreak, Link as LinkIcon } from 'phosphor-react';
import React, { useState } from 'react';
import { Redirect, useHistory } from 'react-router';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityContext } from '@app/entity/shared/EntityContext';
import { DrawerType, GenericEntityProperties } from '@app/entity/shared/types';
import CreateGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import MoveDomainModal from '@app/entityV2/shared/EntityDropdown/MoveDomainModal';
import MoveGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/MoveGlossaryEntityModal';
import { UpdateDeprecationModal } from '@app/entityV2/shared/EntityDropdown/UpdateDeprecationModal';
import useDeleteEntity from '@app/entityV2/shared/EntityDropdown/useDeleteEntity';
import {
    isDeleteDisabled,
    isMoveDisabled,
    shouldDisplayChildDeletionWarning,
} from '@app/entityV2/shared/EntityDropdown/utils';
import LinkAssetVersionModal from '@app/entityV2/shared/EntityDropdown/versioning/LinkAssetVersionModal';
import UnlinkAssetVersionModal from '@app/entityV2/shared/EntityDropdown/versioning/UnlinkAssetVersionModal';
import CreateEntityAnnouncementModal from '@app/entityV2/shared/announce/CreateEntityAnnouncementModal';
import { MarkAsDeprecatedButtonContents } from '@app/entityV2/shared/components/styled/MarkAsDeprecatedButton';
import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { AddIncidentModal } from '@app/entityV2/shared/tabs/Incident/components/AddIncidentModal';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { getEntityProfileDeleteRedirectPath } from '@app/shared/deleteUtils';
import ShareButtonMenu from '@app/shared/share/v2/ShareButtonMenu';
import { useAppConfig, useIsNestedDomainsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useUpdateDeprecationMutation } from '@graphql/mutations.generated';
import { EntityType } from '@types';

const MenuItem = styled.div`
    font-size: 13px;
    font-weight: 400;
    padding: 0 12px;
    color: #46507b;
    line-height: 24px;
    display: flex;
    align-items: center;
    gap: 6px;
`;

export const StyledSubMenu = styled(Menu.SubMenu)`
    .ant-dropdown-menu-submenu-title {
        display: flex;
        align-items: end;
    }
`;

const StyledMoreIcon = styled(MoreVertOutlinedIcon)`
    &&& {
        display: flex;
        font-size: 20px;
        padding: 2px;

        :hover {
            color: ${REDESIGN_COLORS.TITLE_PURPLE};
        }
    }
`;

const StyledMenuItem = styled(Menu.Item)<{ disabled?: boolean }>`
    ${(props) =>
        props.disabled &&
        `
        ${MenuItem} {
            color: ${ANTD_GRAY[7]};
        }
    `}
`;

interface Options {
    hideDeleteMessage?: boolean;
    skipDeleteWait?: boolean;
}

interface Props {
    urn: string;
    entityType: EntityType;
    entityData: GenericEntityProperties | null;
    menuItems: Set<EntityMenuItems>;
    options?: Options;
    refetchForEntity?: () => void;
    refetchForTerms?: () => void;
    refetchForNodes?: () => void;
    onDeleteEntity?: () => void;
    onEditEntity?: () => void;
    triggerType?: ('click' | 'contextMenu' | 'hover')[] | undefined;
}

const EntityDropdown = (props: Props) => {
    const history = useHistory();

    const {
        urn,
        entityData,
        entityType,
        menuItems,
        refetchForEntity,
        refetchForTerms,
        refetchForNodes,
        onDeleteEntity: onDelete,
        onEditEntity: onEdit,
        options,
        triggerType = ['click'],
    } = props;
    const { urn: entityProfileUrn, setDrawer } = useEntityContext();
    const onEntityProfile = entityProfileUrn === urn;

    const me = useUserContext();
    const entityRegistry = useEntityRegistry();
    const versioningEnabled = useAppConfig().config.featureFlags.entityVersioningEnabled;

    const [updateDeprecation] = useUpdateDeprecationMutation();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const { onDeleteEntity, hasBeenDeleted } = useDeleteEntity(
        urn,
        entityType,
        entityData,
        onDelete,
        options?.hideDeleteMessage,
        options?.skipDeleteWait,
    );

    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [isDeprecationModalVisible, setIsDeprecationModalVisible] = useState(false);
    const [isEntityAnnouncementModalVisible, setIsEntityAnnouncementModalVisible] = useState(false);
    const [isMoveModalVisible, setIsMoveModalVisible] = useState(false);
    const [isRaiseIncidentModalVisible, setIsRaiseIncidentModalVisible] = useState(false);
    const [isLinkAssetVersionModalVisible, setIsLinkAssetVersionModalVisible] = useState(false);
    const [isUnlinkAssetVersionModalVisible, setIsUnlinkAssetVersionModalVisible] = useState(false);

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
        refetchForEntity?.();
    };

    const pageUrl = window.location.href;
    const isGlossaryEntity = entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm;
    const isDomainEntity = entityType === EntityType.Domain;
    const canCreateGlossaryEntity = !!entityData?.privileges?.canManageChildren;
    const isDomainMoveHidden = !isNestedDomainsEnabled && isDomainEntity;

    /**
     * A default path to redirect to if the entity is deleted.
     */
    const deleteRedirectPath = getEntityProfileDeleteRedirectPath(entityType, entityData);

    return (
        <>
            <Dropdown
                overlayStyle={{ minWidth: 150 }}
                overlay={
                    <Menu>
                        {menuItems.has(EntityMenuItems.COPY_URL) && navigator.clipboard && (
                            <Menu.Item key="0">
                                <MenuItem
                                    onClick={() => {
                                        navigator.clipboard.writeText(pageUrl);
                                        message.info('Copied URL!', 1.2);
                                    }}
                                >
                                    <LinkOutlined />
                                    &nbsp; Copy Url
                                </MenuItem>
                            </Menu.Item>
                        )}
                        {menuItems.has(EntityMenuItems.UPDATE_DEPRECATION) && (
                            <Menu.Item key="1">
                                {!entityData?.deprecation?.deprecated ? (
                                    <MenuItem onClick={() => setIsDeprecationModalVisible(true)}>
                                        <MarkAsDeprecatedButtonContents />
                                    </MenuItem>
                                ) : (
                                    <MenuItem onClick={() => handleUpdateDeprecation(false)}>
                                        <MarkAsDeprecatedButtonContents internalText="Mark as un-deprecated" />
                                    </MenuItem>
                                )}
                            </Menu.Item>
                        )}
                        {menuItems.has(EntityMenuItems.ANNOUNCE) && (
                            <Menu.Item key="1-1">
                                <MenuItem onClick={() => setIsEntityAnnouncementModalVisible(true)}>
                                    <NotificationOutlined />
                                    &nbsp;Add Note
                                </MenuItem>
                            </Menu.Item>
                        )}
                        {menuItems.has(EntityMenuItems.ADD_TERM) && (
                            <StyledMenuItem
                                key="2"
                                // can not be disabled on acryl-main due to ability to propose
                                onClick={() => setIsCreateTermModalVisible(true)}
                            >
                                <MenuItem>
                                    <PlusOutlined />
                                    &nbsp;Add Term
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.ADD_TERM_GROUP) && (
                            <StyledMenuItem
                                key="3"
                                // can not be disabled on acryl-main due to ability to propose
                                onClick={() => setIsCreateNodeModalVisible(true)}
                            >
                                <MenuItem>
                                    <FolderAddOutlined />
                                    &nbsp;Add Term Group
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {!isDomainMoveHidden && menuItems.has(EntityMenuItems.MOVE) && (
                            <StyledMenuItem
                                data-testid="entity-menu-move-button"
                                key="4"
                                disabled={isMoveDisabled(entityType, entityData, me.platformPrivileges)}
                                onClick={() => setIsMoveModalVisible(true)}
                            >
                                <MenuItem>
                                    <FolderOpenOutlined /> &nbsp;Move
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.DELETE) && (
                            <StyledMenuItem
                                key="5"
                                disabled={isDeleteDisabled(entityType, entityData, me.platformPrivileges)}
                                onClick={onDeleteEntity}
                            >
                                <Tooltip
                                    title={
                                        shouldDisplayChildDeletionWarning(entityType, entityData, me.platformPrivileges)
                                            ? `Can't delete ${entityRegistry.getEntityName(entityType)} with ${
                                                  isDomainEntity ? 'sub-domain' : 'child'
                                              } entities.`
                                            : undefined
                                    }
                                >
                                    <MenuItem data-testid="entity-menu-delete-button">
                                        <DeleteOutlined /> &nbsp;Delete
                                    </MenuItem>
                                </Tooltip>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.EDIT) && onEdit && (
                            <StyledMenuItem key="9" onClick={onEdit}>
                                <MenuItem data-testid="entity-menu-edit-button">
                                    <EditOutlined /> &nbsp;Edit
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.RAISE_INCIDENT) && (
                            <StyledMenuItem key="6" disabled={false}>
                                <MenuItem onClick={() => setIsRaiseIncidentModalVisible(true)}>
                                    <WarningOutlined /> &nbsp;Raise Incident
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {onEntityProfile &&
                            versioningEnabled &&
                            menuItems.has(EntityMenuItems.LINK_VERSION) &&
                            !entityData?.versionProperties && (
                                <StyledMenuItem key="link" disabled={false}>
                                    <MenuItem onClick={() => setIsLinkAssetVersionModalVisible(true)}>
                                        <LinkIcon fontSize="inherit" /> &nbsp;Link a Newer Version
                                    </MenuItem>
                                </StyledMenuItem>
                            )}
                        {onEntityProfile && entityData?.versionProperties?.isLatest && (
                            <StyledMenuItem key="unlink" disabled={false}>
                                <MenuItem onClick={() => setIsUnlinkAssetVersionModalVisible(true)}>
                                    <LinkBreak fontSize="inherit" /> &nbsp;Unlink from Previous Version
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {onEntityProfile && entityData?.versionProperties && setDrawer && (
                            <StyledMenuItem key="showVersions" disabled={false}>
                                <MenuItem
                                    onClick={() => {
                                        analytics.event({
                                            type: EventType.ShowAllVersionsEvent,
                                            assetUrn: urn,
                                            versionSetUrn: entityData?.versionProperties?.versionSet?.urn,
                                            entityType,
                                            uiLocation: 'preview',
                                        });
                                        setDrawer(DrawerType.VERSIONS);
                                    }}
                                >
                                    <GitCommit fontSize="inherit" /> &nbsp;Show Versions
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.SHARE) && (
                            <StyledSubMenu
                                key="8"
                                disabled={false}
                                title={
                                    <MenuItem>
                                        <ShareAltOutlined /> &nbsp;Share
                                    </MenuItem>
                                }
                            >
                                <ShareButtonMenu
                                    urn={urn}
                                    entityType={entityType}
                                    subType={
                                        (entityData?.subTypes?.typeNames?.length &&
                                            entityData?.subTypes?.typeNames?.[0]) ||
                                        undefined
                                    }
                                    name={entityData?.name}
                                />
                            </StyledSubMenu>
                        )}{' '}
                    </Menu>
                }
                trigger={triggerType}
            >
                <StyledMoreIcon />
            </Dropdown>
            {isCreateTermModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryTerm}
                    canCreateGlossaryEntity={canCreateGlossaryEntity}
                    onClose={() => setIsCreateTermModalVisible(false)}
                    refetchData={refetchForTerms}
                />
            )}
            {isCreateNodeModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryNode}
                    canCreateGlossaryEntity={canCreateGlossaryEntity}
                    onClose={() => setIsCreateNodeModalVisible(false)}
                    refetchData={refetchForNodes}
                />
            )}
            {isDeprecationModalVisible && (
                <UpdateDeprecationModal
                    urns={[urn]}
                    onClose={() => setIsDeprecationModalVisible(false)}
                    refetch={refetchForEntity}
                />
            )}
            {isEntityAnnouncementModalVisible && (
                <CreateEntityAnnouncementModal
                    urn={urn}
                    onClose={() => setIsEntityAnnouncementModalVisible(false)}
                    onCreate={() => setTimeout(() => refetchForEntity?.(), 2000)}
                />
            )}
            {isMoveModalVisible && isGlossaryEntity && (
                <MoveGlossaryEntityModal
                    entityData={entityData}
                    urn={urn}
                    entityType={entityType}
                    onClose={() => setIsMoveModalVisible(false)}
                />
            )}
            {isMoveModalVisible && isDomainEntity && <MoveDomainModal onClose={() => setIsMoveModalVisible(false)} />}
            {hasBeenDeleted && !onDelete && deleteRedirectPath && <Redirect to={deleteRedirectPath} />}
            {isRaiseIncidentModalVisible && (
                <AddIncidentModal
                    urn={urn}
                    entityType={entityType}
                    visible={isRaiseIncidentModalVisible}
                    onClose={() => setIsRaiseIncidentModalVisible(false)}
                    refetch={
                        (() => {
                            refetchForEntity?.();
                            history.push(
                                `${getEntityPath(
                                    entityType,
                                    urn,
                                    entityRegistry,
                                    false,
                                    isHideSiblingMode,
                                    'Incidents',
                                )}`,
                            );
                        }) as any
                    }
                />
            )}
            {isLinkAssetVersionModalVisible && (
                <LinkAssetVersionModal
                    urn={urn}
                    entityType={entityType}
                    closeModal={() => setIsLinkAssetVersionModalVisible(false)}
                    refetch={refetchForEntity}
                />
            )}
            {isUnlinkAssetVersionModalVisible && (
                <UnlinkAssetVersionModal
                    urn={urn}
                    entityType={entityType}
                    versionSetUrn={entityData?.versionProperties?.versionSet?.urn}
                    closeModal={() => setIsUnlinkAssetVersionModalVisible(false)}
                    refetch={refetchForEntity}
                />
            )}
        </>
    );
};

export default EntityDropdown;
