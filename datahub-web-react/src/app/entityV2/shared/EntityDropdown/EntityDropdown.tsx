import React, { useState } from 'react';
import styled from 'styled-components';
import { Dropdown, Menu, message, Tooltip } from 'antd';
import {
    DeleteOutlined,
    ExclamationCircleOutlined,
    FolderAddOutlined,
    FolderOpenOutlined,
    LinkOutlined,
    MoreOutlined,
    PlusOutlined,
    WarningOutlined,
    ShareAltOutlined,
    BellOutlined,
    BellFilled,
} from '@ant-design/icons';
import MoreVertOutlinedIcon from '@mui/icons-material/MoreVertOutlined';
import { Redirect, useHistory } from 'react-router';
import { EntityType } from '../../../../types.generated';
import CreateGlossaryEntityModal from './CreateGlossaryEntityModal';
import { UpdateDeprecationModal } from './UpdateDeprecationModal';
import { useUpdateDeprecationMutation } from '../../../../graphql/mutations.generated';
import MoveGlossaryEntityModal from './MoveGlossaryEntityModal';
import { ANTD_GRAY, REDESIGN_COLORS } from '../constants';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { AddIncidentModal } from '../tabs/Incident/components/AddIncidentModal';
import { getEntityPath } from '../containers/profile/utils';
import useDeleteEntity from './useDeleteEntity';
import { getEntityProfileDeleteRedirectPath } from '../../../shared/deleteUtils';
import { useIsSeparateSiblingsMode } from '../siblingUtils';
import { shouldDisplayChildDeletionWarning, isDeleteDisabled, isMoveDisabled } from './utils';
import { useUserContext } from '../../../context/useUserContext';
import MoveDomainModal from './MoveDomainModal';
import { useIsNestedDomainsEnabled } from '../../../useAppConfig';
import { EntityMenuItems } from './EntityMenuActions';
import ShareButtonMenu from '../../../shared/share/v2/ShareButtonMenu';
import SubscribeButtonMenu from '../../../shared/subscribe/v2/SubscribeButtonMenu';
import useSubscriptionSummary from '../../../shared/subscribe/useSubscriptionSummary';

export const MenuIcon = styled(MoreOutlined)<{ fontSize?: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: ${(props) => props.fontSize || '24'}px;
    height: 32px;
    margin-left: 5px;
`;

const MenuItem = styled.div`
    font-size: 13px;
    font-weight: 400;
    padding: 0 4px;
    color: #46507b;
    line-height: 24px;
    display: flex;
    align-items: center;
    gap: 6px;
`;

const StyledSubMenu = styled(Menu.SubMenu)`
    .ant-dropdown-menu-submenu-title {
        padding-right: 0px;
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
        props.disabled
            ? `
            ${MenuItem} {
                color: ${ANTD_GRAY[7]};
            }
    `
            : ''}
`;

interface Options {
    hideDeleteMessage?: boolean;
    skipDeleteWait?: boolean;
}

interface Props {
    urn: string;
    entityType: EntityType;
    entityData?: any;
    menuItems: Set<EntityMenuItems>;
    options?: Options;
    refetchForEntity?: () => void;
    refetchForTerms?: () => void;
    refetchForNodes?: () => void;
    onDeleteEntity?: () => void;
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
        options,
    } = props;

    const me = useUserContext();
    const entityRegistry = useEntityRegistry();
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

    const { isUserSubscribed, setIsUserSubscribed, refetchSubscriptionSummary } = useSubscriptionSummary({
        entityUrn: urn,
    });

    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [isDeprecationModalVisible, setIsDeprecationModalVisible] = useState(false);
    const [isMoveModalVisible, setIsMoveModalVisible] = useState(false);
    // acryl-main only
    const [isRaiseIncidentModalVisible, setIsRaiseIncidentModalVisible] = useState(false);

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
                                        <ExclamationCircleOutlined />
                                        &nbsp; Mark as deprecated
                                    </MenuItem>
                                ) : (
                                    <MenuItem onClick={() => handleUpdateDeprecation(false)}>
                                        <ExclamationCircleOutlined />
                                        &nbsp; Mark as un-deprecated
                                    </MenuItem>
                                )}
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
                        {menuItems.has(EntityMenuItems.RAISE_INCIDENT) && (
                            <StyledMenuItem key="6" disabled={false}>
                                <MenuItem onClick={() => setIsRaiseIncidentModalVisible(true)}>
                                    <WarningOutlined /> &nbsp;Raise Incident
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.SUBSCRIBE) && (
                            <StyledSubMenu
                                key="7"
                                disabled={false}
                                title={
                                    <MenuItem>
                                        {isUserSubscribed ? <BellFilled /> : <BellOutlined />} &nbsp;Subscribe
                                    </MenuItem>
                                }
                            >
                                <SubscribeButtonMenu
                                    isUserSubscribed={isUserSubscribed}
                                    setIsUserSubscribed={setIsUserSubscribed}
                                    refetchSubscriptionSummary={refetchSubscriptionSummary}
                                    entityUrn={urn}
                                />
                            </StyledSubMenu>
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
                        )}
                    </Menu>
                }
                trigger={['click']}
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
            {isMoveModalVisible && isGlossaryEntity && (
                <MoveGlossaryEntityModal onClose={() => setIsMoveModalVisible(false)} />
            )}
            {isMoveModalVisible && isDomainEntity && <MoveDomainModal onClose={() => setIsMoveModalVisible(false)} />}
            {hasBeenDeleted && !onDelete && deleteRedirectPath && <Redirect to={deleteRedirectPath} />}
            {/* acryl-main only */}
            {isRaiseIncidentModalVisible && (
                <AddIncidentModal
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
        </>
    );
};

export default EntityDropdown;
