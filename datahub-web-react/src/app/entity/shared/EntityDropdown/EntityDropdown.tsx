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
} from '@ant-design/icons';
import { Redirect } from 'react-router';
import { EntityType } from '../../../../types.generated';
import CreateGlossaryEntityModal from './CreateGlossaryEntityModal';
import { UpdateDeprecationModal } from './UpdateDeprecationModal';
import { useUpdateDeprecationMutation } from '../../../../graphql/mutations.generated';
import MoveGlossaryEntityModal from './MoveGlossaryEntityModal';
import { ANTD_GRAY } from '../constants';
import { useEntityRegistry } from '../../../useEntityRegistry';
import useDeleteEntity from './useDeleteEntity';
import { getEntityProfileDeleteRedirectPath } from '../../../shared/deleteUtils';

export enum EntityMenuItems {
    COPY_URL,
    UPDATE_DEPRECATION,
    ADD_TERM,
    ADD_TERM_GROUP,
    DELETE,
    MOVE,
}

export const MenuIcon = styled(MoreOutlined)<{ fontSize?: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: ${(props) => props.fontSize || '24'}px;
    height: 32px;
    margin-left: 5px;
`;

const MenuItem = styled.div`
    font-size: 12px;
    padding: 0 4px;
    color: #262626;
`;

const StyledMenuItem = styled(Menu.Item)<{ disabled: boolean }>`
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
    size?: number;
    options?: Options;
    refetchForEntity?: () => void;
    refetchForTerms?: () => void;
    refetchForNodes?: () => void;
    refreshBrowser?: () => void;
    onDeleteEntity?: () => void;
}

function EntityDropdown(props: Props) {
    const {
        urn,
        entityData,
        entityType,
        menuItems,
        refetchForEntity,
        refetchForTerms,
        refetchForNodes,
        refreshBrowser,
        onDeleteEntity: onDelete,
        size,
        options,
    } = props;

    const entityRegistry = useEntityRegistry();
    const [updateDeprecation] = useUpdateDeprecationMutation();
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
    const [isMoveModalVisible, setIsMoveModalVisible] = useState(false);

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
    const entityHasChildren = !!entityData?.children?.total;
    const canManageGlossaryEntity = !!entityData?.privileges?.canManageEntity;
    const canCreateGlossaryEntity = !!entityData?.privileges?.canManageChildren;
    const canDeleteGlossaryEntity = !entityHasChildren && canManageGlossaryEntity;

    /**
     * A default path to redirect to if the entity is deleted.
     */
    const deleteRedirectPath = getEntityProfileDeleteRedirectPath(entityType);

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
                                    <LinkOutlined /> &nbsp; Copy Url
                                </MenuItem>
                            </Menu.Item>
                        )}
                        {menuItems.has(EntityMenuItems.UPDATE_DEPRECATION) && (
                            <Menu.Item key="1">
                                {!entityData?.deprecation?.deprecated ? (
                                    <MenuItem onClick={() => setIsDeprecationModalVisible(true)}>
                                        <ExclamationCircleOutlined /> &nbsp; Mark as deprecated
                                    </MenuItem>
                                ) : (
                                    <MenuItem onClick={() => handleUpdateDeprecation(false)}>
                                        <ExclamationCircleOutlined /> &nbsp; Mark as un-deprecated
                                    </MenuItem>
                                )}
                            </Menu.Item>
                        )}
                        {menuItems.has(EntityMenuItems.ADD_TERM) && (
                            <StyledMenuItem
                                key="2"
                                disabled={!canCreateGlossaryEntity}
                                onClick={() => setIsCreateTermModalVisible(true)}
                            >
                                <MenuItem>
                                    <PlusOutlined /> &nbsp;Add Term
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.ADD_TERM_GROUP) && (
                            <StyledMenuItem
                                key="3"
                                disabled={!canCreateGlossaryEntity}
                                onClick={() => setIsCreateNodeModalVisible(true)}
                            >
                                <MenuItem>
                                    <FolderAddOutlined /> &nbsp;Add Term Group
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.MOVE) && (
                            <StyledMenuItem
                                key="4"
                                disabled={!canManageGlossaryEntity}
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
                                disabled={isGlossaryEntity && !canDeleteGlossaryEntity}
                                onClick={onDeleteEntity}
                            >
                                <Tooltip
                                    title={`Can't delete ${entityRegistry.getEntityName(
                                        entityType,
                                    )} with child entities.`}
                                    overlayStyle={
                                        canManageGlossaryEntity && entityHasChildren ? {} : { display: 'none' }
                                    }
                                >
                                    <MenuItem>
                                        <DeleteOutlined /> &nbsp;Delete
                                    </MenuItem>
                                </Tooltip>
                            </StyledMenuItem>
                        )}
                    </Menu>
                }
                trigger={['click']}
            >
                <MenuIcon data-testid="entity-header-dropdown" fontSize={size} />
            </Dropdown>
            {isCreateTermModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryTerm}
                    onClose={() => setIsCreateTermModalVisible(false)}
                    refetchData={refetchForTerms}
                />
            )}
            {isCreateNodeModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryNode}
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
            {isMoveModalVisible && (
                <MoveGlossaryEntityModal onClose={() => setIsMoveModalVisible(false)} refetchData={refreshBrowser} />
            )}
            {hasBeenDeleted && !onDelete && deleteRedirectPath && <Redirect to={deleteRedirectPath} />}
        </>
    );
}

export default EntityDropdown;
