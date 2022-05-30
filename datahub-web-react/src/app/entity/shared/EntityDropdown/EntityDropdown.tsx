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
import { EntityType, PlatformPrivileges } from '../../../../types.generated';
import CreateGlossaryEntityModal from './CreateGlossaryEntityModal';
import { AddDeprecationDetailsModal } from './AddDeprecationDetailsModal';
import { useEntityData, useRefetch } from '../EntityContext';
import { useUpdateDeprecationMutation } from '../../../../graphql/mutations.generated';
import MoveGlossaryEntityModal from './MoveGlossaryEntityModal';
import useDeleteGlossaryEntity from './useDeleteGlossaryEntity';
import { PageRoutes } from '../../../../conf/Global';
import { ANTD_GRAY } from '../constants';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useGetAuthenticatedUser } from '../../../useGetAuthenticatedUser';

export enum EntityMenuItems {
    COPY_URL,
    UPDATE_DEPRECATION,
    ADD_TERM,
    ADD_TERM_GROUP,
    DELETE,
    MOVE,
}

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

interface Props {
    menuItems: Set<EntityMenuItems>;
    platformPrivileges?: PlatformPrivileges;
    refetchForTerms?: () => void;
    refetchForNodes?: () => void;
    refreshBrowser?: () => void;
}

function EntityDropdown(props: Props) {
    const { menuItems, platformPrivileges, refetchForTerms, refetchForNodes, refreshBrowser } = props;

    const entityRegistry = useEntityRegistry();
    const { urn, entityData, entityType } = useEntityData();
    const refetch = useRefetch();
    const me = useGetAuthenticatedUser(!!platformPrivileges);
    const [updateDeprecation] = useUpdateDeprecationMutation();
    const { onDeleteEntity, hasBeenDeleted } = useDeleteGlossaryEntity();

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
        refetch?.();
    };

    const canManageGlossaries = platformPrivileges
        ? platformPrivileges.manageDomains
        : me?.platformPrivileges.manageDomains;
    const pageUrl = window.location.href;
    const isDeleteDisabled = !!entityData?.children?.count;

    return (
        <>
            <Dropdown
                overlay={
                    <Menu>
                        {menuItems.has(EntityMenuItems.COPY_URL) && (
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
                            <StyledMenuItem key="2" disabled={!canManageGlossaries}>
                                <MenuItem onClick={() => setIsCreateTermModalVisible(true)}>
                                    <PlusOutlined /> &nbsp;Add Term
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.ADD_TERM_GROUP) && (
                            <StyledMenuItem key="3" disabled={!canManageGlossaries}>
                                <MenuItem onClick={() => setIsCreateNodeModalVisible(true)}>
                                    <FolderAddOutlined /> &nbsp;Add Term Group
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.MOVE) && (
                            <StyledMenuItem key="4" disabled={!canManageGlossaries}>
                                <MenuItem onClick={() => setIsMoveModalVisible(true)}>
                                    <FolderOpenOutlined /> &nbsp;Move
                                </MenuItem>
                            </StyledMenuItem>
                        )}
                        {menuItems.has(EntityMenuItems.DELETE) && (
                            <StyledMenuItem key="5" disabled={isDeleteDisabled || !canManageGlossaries}>
                                <Tooltip
                                    title={`Can't delete ${entityRegistry.getEntityName(
                                        entityType,
                                    )} with child entities.`}
                                    overlayStyle={isDeleteDisabled ? {} : { display: 'none' }}
                                >
                                    <MenuItem onClick={onDeleteEntity}>
                                        <DeleteOutlined /> &nbsp;Delete
                                    </MenuItem>
                                </Tooltip>
                            </StyledMenuItem>
                        )}
                    </Menu>
                }
                trigger={['click']}
            >
                <MenuIcon />
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
                <AddDeprecationDetailsModal
                    visible={isDeprecationModalVisible}
                    urn={urn}
                    onClose={() => setIsDeprecationModalVisible(false)}
                    refetch={refetch}
                />
            )}
            {isMoveModalVisible && (
                <MoveGlossaryEntityModal onClose={() => setIsMoveModalVisible(false)} refetchData={refreshBrowser} />
            )}
            {hasBeenDeleted && <Redirect to={`${PageRoutes.GLOSSARY}`} />}
        </>
    );
}

export default EntityDropdown;
