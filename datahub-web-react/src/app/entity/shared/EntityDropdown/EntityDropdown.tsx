import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, Dropdown, Menu, message } from 'antd';
import { Tooltip } from '@components';
import { Redirect, useHistory } from 'react-router';
import {
    Copy,
    CopySimple,
    DotsThreeVertical,
    FolderOpen,
    FolderPlus,
    Link,
    PlusCircle,
    Trash,
    Warning,
} from 'phosphor-react';
import { EntityType } from '../../../../types.generated';
import CreateGlossaryEntityModal from './CreateGlossaryEntityModal';
import { UpdateDeprecationModal } from './UpdateDeprecationModal';
import { useUpdateDeprecationMutation } from '../../../../graphql/mutations.generated';
import MoveGlossaryEntityModal from './MoveGlossaryEntityModal';
import { ANTD_GRAY } from '../constants';
import { useEntityRegistry } from '../../../useEntityRegistry';
import useDeleteEntity from './useDeleteEntity';
import { getEntityProfileDeleteRedirectPath } from '../../../shared/deleteUtils';
import { shouldDisplayChildDeletionWarning, isDeleteDisabled, isMoveDisabled } from './utils';
import { useUserContext } from '../../../context/useUserContext';
import MoveDomainModal from './MoveDomainModal';
import { useIsNestedDomainsEnabled } from '../../../useAppConfig';
import { getEntityPath } from '../containers/profile/utils';
import { useIsSeparateSiblingsMode } from '../siblingUtils';
import { AddIncidentModal } from '../tabs/Incident/components/AddIncidentModal';
import DeprecatedIcon from '../../../../images/deprecated-status.svg?react';

export enum EntityMenuItems {
    COPY_URL,
    COPY_URN,
    UPDATE_DEPRECATION,
    ADD_TERM,
    ADD_TERM_GROUP,
    DELETE,
    MOVE,
    CLONE,
    RAISE_INCIDENT,
}

export const MenuIcon = styled(DotsThreeVertical)<{ fontSize?: number }>`
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

const StyledMenuItem = styled(Menu.Item)<{ disabled?: boolean }>`
    &&&& {
        background-color: transparent;
    }
    ${(props) =>
        props.disabled
            ? `
            ${MenuItem} {
                color: ${ANTD_GRAY[7]};
            }
    `
            : ''}
`;

const StyledDeprecatedIcon = styled(DeprecatedIcon)`
    color: inherit;
    path {
        fill: currentColor;
    }
    && {
        fill: currentColor;
    }
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
    onDeleteEntity?: () => void;
}

function EntityDropdown(props: Props) {
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
        size,
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

    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [isCloneEntityModalVisible, setIsCloneEntityModalVisible] = useState<boolean>(false);
    const [isDeprecationModalVisible, setIsDeprecationModalVisible] = useState(false);
    const [isMoveModalVisible, setIsMoveModalVisible] = useState(false);
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

    const items = [
        menuItems.has(EntityMenuItems.COPY_URL) && navigator.clipboard
            ? {
                  key: 0,
                  label: (
                      <StyledMenuItem
                          onClick={() => {
                              navigator.clipboard.writeText(pageUrl);
                              message.info('Copied URL!', 1.2);
                          }}
                      >
                          <MenuItem>
                              {/* eslint-disable-next-line jsx-a11y/anchor-is-valid */}
                              <Link /> &nbsp; Copy Url
                          </MenuItem>
                      </StyledMenuItem>
                  ),
              }
            : null,
        menuItems.has(EntityMenuItems.COPY_URN) && navigator.clipboard
            ? {
                  key: 1,
                  label: (
                      <StyledMenuItem
                          onClick={() => {
                              navigator.clipboard.writeText(urn);
                              message.info('Copied URN!', 1.2);
                          }}
                      >
                          <MenuItem>
                              <Copy /> &nbsp; Copy URN
                          </MenuItem>
                      </StyledMenuItem>
                  ),
              }
            : null,
        menuItems.has(EntityMenuItems.UPDATE_DEPRECATION)
            ? {
                  key: 2,
                  label: !entityData?.deprecation?.deprecated ? (
                      <MenuItem onClick={() => setIsDeprecationModalVisible(true)}>
                          <StyledDeprecatedIcon /> &nbsp; Mark as deprecated
                      </MenuItem>
                  ) : (
                      <MenuItem onClick={() => handleUpdateDeprecation(false)}>
                          <StyledDeprecatedIcon /> &nbsp; Mark as un-deprecated
                      </MenuItem>
                  ),
              }
            : null,
        menuItems.has(EntityMenuItems.ADD_TERM)
            ? {
                  key: 3,
                  label: (
                      <StyledMenuItem
                          data-testid="entity-menu-add-term-button"
                          key="2"
                          // can not be disabled on acryl-main due to ability to propose
                          onClick={() => setIsCreateTermModalVisible(true)}
                      >
                          <MenuItem>
                              <PlusCircle /> &nbsp;Add Term
                          </MenuItem>
                      </StyledMenuItem>
                  ),
              }
            : null,
        menuItems.has(EntityMenuItems.ADD_TERM_GROUP)
            ? {
                  key: 4,
                  label: (
                      <StyledMenuItem
                          key="3"
                          // can not be disabled on acryl-main due to ability to propose
                          onClick={() => setIsCreateNodeModalVisible(true)}
                      >
                          <MenuItem>
                              <FolderPlus /> &nbsp;Add Term Group
                          </MenuItem>
                      </StyledMenuItem>
                  ),
              }
            : null,
        !isDomainMoveHidden && menuItems.has(EntityMenuItems.MOVE)
            ? {
                  key: 5,
                  label: (
                      <StyledMenuItem
                          data-testid="entity-menu-move-button"
                          key="4"
                          disabled={isMoveDisabled(entityType, entityData, me.platformPrivileges)}
                          onClick={() => setIsMoveModalVisible(true)}
                      >
                          <MenuItem>
                              <FolderOpen /> &nbsp;Move
                          </MenuItem>
                      </StyledMenuItem>
                  ),
              }
            : null,
        menuItems.has(EntityMenuItems.DELETE)
            ? {
                  key: 6,
                  label: (
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
                                  <Trash /> &nbsp;Delete
                              </MenuItem>
                          </Tooltip>
                      </StyledMenuItem>
                  ),
              }
            : null,
        menuItems.has(EntityMenuItems.CLONE)
            ? {
                  key: 7,
                  label: (
                      <StyledMenuItem
                          key="6"
                          disabled={!entityData?.privileges?.canManageEntity}
                          onClick={() => setIsCloneEntityModalVisible(true)}
                      >
                          <MenuItem>
                              <CopySimple /> &nbsp;Clone
                          </MenuItem>
                      </StyledMenuItem>
                  ),
              }
            : null,
        menuItems.has(EntityMenuItems.RAISE_INCIDENT)
            ? {
                  key: 8,
                  label: (
                      <StyledMenuItem key="6" disabled={false}>
                          <MenuItem onClick={() => setIsRaiseIncidentModalVisible(true)}>
                              <Warning /> &nbsp;Raise Incident
                          </MenuItem>
                      </StyledMenuItem>
                  ),
              }
            : null,
    ];

    return (
        <>
            <Dropdown menu={{ items }} trigger={['click']}>
                <Button
                    type="text"
                    style={{ padding: 0 }}
                    onClick={(e) => e.preventDefault()}
                    data-testid="entity-header-dropdown"
                >
                    <MenuIcon fontSize={size} />
                </Button>
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
            {isCloneEntityModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={entityType}
                    canCreateGlossaryEntity={canCreateGlossaryEntity}
                    onClose={() => setIsCloneEntityModalVisible(false)}
                    refetchData={entityType === EntityType.GlossaryTerm ? refetchForTerms : refetchForNodes}
                    isCloning
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
            {isRaiseIncidentModalVisible && (
                <AddIncidentModal
                    open={isRaiseIncidentModalVisible}
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
}

export default EntityDropdown;
