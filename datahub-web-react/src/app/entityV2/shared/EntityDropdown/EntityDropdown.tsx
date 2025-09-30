import { Menu } from '@components';
import MoreVertOutlinedIcon from '@mui/icons-material/MoreVertOutlined';
import { message } from 'antd';
import qs from 'query-string';
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
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { IncidentDetailDrawer } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentDetailDrawer';
import { IncidentAction } from '@app/entityV2/shared/tabs/Incident/constant';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { getFirstSubType } from '@app/entityV2/shared/utils';
import { getEntityProfileDeleteRedirectPath } from '@app/shared/deleteUtils';
import { useAppConfig, useIsNestedDomainsEnabled } from '@app/useAppConfig';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useUpdateDeprecationMutation } from '@graphql/mutations.generated';
import { EntityType } from '@types';

import DeprecatedIcon from '@images/deprecated-status.svg?react';

const StyledMoreIcon = styled(MoreVertOutlinedIcon)`
    &&& {
        display: flex;
        font-size: 20px;
        padding: 2px;

        :hover {
            color: ${(p) => p.theme.styles['primary-color']};
        }
    }
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
    const entityRegistryV2 = useEntityRegistryV2();
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
            analytics.event({
                type: EventType.SetDeprecation,
                entityUrns: [urn],
                deprecated: deprecatedStatus,
            });
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

    // Build menu items for the new Menu component
    const menuItemsList: any[] = [];

    if (menuItems.has(EntityMenuItems.COPY_URL) && navigator.clipboard) {
        menuItemsList.push({
            type: 'item' as const,
            key: '0',
            title: 'Copy Url',
            icon: 'Link',
            onClick: () => {
                navigator.clipboard.writeText(pageUrl);
                message.info('Copied URL!', 1.2);
            },
        });
    }

    if (menuItems.has(EntityMenuItems.UPDATE_DEPRECATION)) {
        menuItemsList.push({
            type: 'item' as const,
            key: '1',
            title: !entityData?.deprecation?.deprecated ? 'Mark as Deprecated' : 'Mark as un-deprecated',
            render: () => (
                <div
                    data-testid="entity-menu-deprecate-button"
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        padding: '8px',
                        gap: '8px',
                    }}
                >
                    <div
                        style={{
                            display: 'flex',
                            flexShrink: 0,
                            width: '20px',
                            height: '20px',
                            alignItems: 'center',
                            justifyContent: 'center',
                        }}
                    >
                        <DeprecatedIcon
                            style={{
                                width: '16px',
                                height: '16px',
                                color: '#8088A3',
                            }}
                        />
                    </div>
                    <div
                        style={{
                            display: 'flex',
                            flexDirection: 'column',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                        }}
                    >
                        <span
                            style={{
                                fontFamily: 'Mulish',
                                fontWeight: 600,
                                color: '#374066',
                                fontSize: '14px',
                            }}
                        >
                            {!entityData?.deprecation?.deprecated ? 'Mark as Deprecated' : 'Mark as un-deprecated'}
                        </span>
                    </div>
                </div>
            ),
            onClick: () => {
                if (!entityData?.deprecation?.deprecated) {
                    setIsDeprecationModalVisible(true);
                } else {
                    handleUpdateDeprecation(false);
                }
            },
        });
    }

    if (menuItems.has(EntityMenuItems.ANNOUNCE)) {
        menuItemsList.push({
            type: 'item' as const,
            key: '1-1',
            title: 'Add Note',
            icon: 'MegaphoneSimple',
            onClick: () => setIsEntityAnnouncementModalVisible(true),
        });
    }

    if (menuItems.has(EntityMenuItems.ADD_TERM)) {
        menuItemsList.push({
            type: 'item' as const,
            key: '2',
            title: 'Add Term',
            icon: 'Plus',
            onClick: () => setIsCreateTermModalVisible(true),
            'data-testid': 'entity-menu-add-term-button',
        });
    }

    if (menuItems.has(EntityMenuItems.ADD_TERM_GROUP)) {
        menuItemsList.push({
            type: 'item' as const,
            key: '3',
            title: 'Add Term Group',
            icon: 'FolderPlus',
            onClick: () => setIsCreateNodeModalVisible(true),
        });
    }

    if (!isDomainMoveHidden && menuItems.has(EntityMenuItems.MOVE)) {
        menuItemsList.push({
            type: 'item' as const,
            key: '4',
            title: 'Move',
            icon: 'FolderOpen',
            disabled: isMoveDisabled(entityType, entityData, me.platformPrivileges),
            onClick: () => setIsMoveModalVisible(true),
            'data-testid': 'entity-menu-move-button',
        });
    }

    if (menuItems.has(EntityMenuItems.EDIT) && onEdit) {
        menuItemsList.push({
            type: 'item' as const,
            key: '9',
            title: 'Edit',
            icon: 'Pencil',
            onClick: onEdit,
        });
    }

    if (menuItems.has(EntityMenuItems.RAISE_INCIDENT)) {
        menuItemsList.push({
            type: 'item' as const,
            key: '6',
            title: 'Raise Incident',
            icon: 'Warning',
            onClick: () => setIsRaiseIncidentModalVisible(true),
        });
    }

    if (
        onEntityProfile &&
        versioningEnabled &&
        menuItems.has(EntityMenuItems.LINK_VERSION) &&
        !entityData?.versionProperties
    ) {
        menuItemsList.push({
            type: 'item' as const,
            key: 'link',
            title: 'Link a Newer Version',
            icon: 'Link',
            onClick: () => setIsLinkAssetVersionModalVisible(true),
        });
    }

    if (onEntityProfile && entityData?.versionProperties?.isLatest) {
        menuItemsList.push({
            type: 'item' as const,
            key: 'unlink',
            title: 'Unlink from Previous Version',
            icon: 'LinkBreak',
            onClick: () => setIsUnlinkAssetVersionModalVisible(true),
        });
    }

    if (onEntityProfile && entityData?.versionProperties && setDrawer) {
        menuItemsList.push({
            type: 'item' as const,
            key: 'showVersions',
            title: 'Show Versions',
            icon: 'GitCommit',
            onClick: () => {
                analytics.event({
                    type: EventType.ShowAllVersionsEvent,
                    assetUrn: urn,
                    versionSetUrn: entityData?.versionProperties?.versionSet?.urn,
                    entityType,
                    uiLocation: 'preview',
                });
                setDrawer(DrawerType.VERSIONS);
            },
        });
    }

    if (menuItems.has(EntityMenuItems.SHARE)) {
        const shareChildren: any[] = [];

        // Copy Link
        if (navigator.clipboard) {
            shareChildren.push({
                type: 'item' as const,
                key: 'copy-link',
                title: 'Copy Link',
                icon: 'Link',
                onClick: () => {
                    const { origin } = window.location;
                    const copyUrl = `${origin}${entityRegistryV2.getEntityUrl(entityType, urn)}/`;
                    navigator.clipboard.writeText(copyUrl);
                },
            });
        }

        // Copy URN
        if (navigator.clipboard) {
            shareChildren.push({
                type: 'item' as const,
                key: 'copy-urn',
                title: 'Copy URN',
                icon: 'Copy',
                onClick: () => {
                    navigator.clipboard.writeText(urn);
                },
            });
        }

        // Copy Name
        if (navigator.clipboard) {
            const displayName = entityData?.name || urn;
            shareChildren.push({
                type: 'item' as const,
                key: 'copy-name',
                title: 'Copy Name',
                icon: 'Copy',
                onClick: () => {
                    const qualifiedName = entityData?.properties?.qualifiedName;
                    if (qualifiedName) {
                        navigator.clipboard.writeText(qualifiedName);
                    } else {
                        navigator.clipboard.writeText(displayName);
                    }
                },
            });
        }

        // Email
        shareChildren.push({
            type: 'item' as const,
            key: 'email',
            title: 'Email',
            icon: 'Envelope',
            onClick: () => {
                const displayName = entityData?.name || urn;
                const displayType =
                    getFirstSubType(entityData) || entityRegistryV2.getEntityName(entityType) || entityType;
                const linkText = window.location.href;
                const link = qs.stringifyUrl({
                    url: 'mailto:',
                    query: {
                        subject: `${displayName} | ${displayType}`,
                        body: `Check out this ${displayType} on DataHub: ${linkText}. Urn: ${urn}`,
                    },
                });
                window.open(link, '_blank', 'noopener,noreferrer');
            },
        });

        menuItemsList.push({
            type: 'item' as const,
            key: '8',
            title: 'Share',
            icon: 'Share',
            children: shareChildren,
        });
    }

    // Delete should always be last (destructive action)
    if (menuItems.has(EntityMenuItems.DELETE)) {
        menuItemsList.push({
            type: 'item' as const,
            key: 'delete',
            title: 'Delete',
            icon: 'Trash',
            danger: true,
            disabled: isDeleteDisabled(entityType, entityData, me.platformPrivileges),
            tooltip: shouldDisplayChildDeletionWarning(entityType, entityData, me.platformPrivileges)
                ? `Can't delete ${entityRegistryV2.getEntityName(entityType)} with ${
                      isDomainEntity ? 'sub-domain' : 'child'
                  } entities.`
                : undefined,
            onClick: onDeleteEntity,
            'data-testid': 'entity-menu-delete-button',
        });
    }

    return (
        <>
            <Menu items={menuItemsList} trigger={triggerType} overlayStyle={{ minWidth: 150 }}>
                <StyledMoreIcon />
            </Menu>
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
                <IncidentDetailDrawer
                    entity={{
                        urn,
                        entityType,
                        platform: entityData?.platform ?? entityData?.dataPlatformInstance?.platform,
                    }}
                    mode={IncidentAction.CREATE}
                    onSubmit={() => {
                        setIsRaiseIncidentModalVisible(false);
                        setTimeout(() => {
                            refetchForEntity?.();
                            history.push(
                                `${getEntityPath(
                                    entityType,
                                    urn,
                                    entityRegistryV2,
                                    false,
                                    isHideSiblingMode,
                                    'Incidents',
                                )}`,
                            );
                        }, 3000);
                    }}
                    onCancel={() => setIsRaiseIncidentModalVisible(false)}
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
