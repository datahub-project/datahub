import { Tooltip } from '@components';
import { CopySimple } from '@phosphor-icons/react/dist/csr/CopySimple';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { FolderOpen } from '@phosphor-icons/react/dist/csr/FolderOpen';
import { FolderPlus } from '@phosphor-icons/react/dist/csr/FolderPlus';
import { Link } from '@phosphor-icons/react/dist/csr/Link';
import { PlusCircle } from '@phosphor-icons/react/dist/csr/PlusCircle';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import { Button, Dropdown, Menu, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Redirect, useHistory } from 'react-router';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import CreateGlossaryEntityModal from '@app/entity/shared/EntityDropdown/CreateGlossaryEntityModal';
import MoveDomainModal from '@app/entity/shared/EntityDropdown/MoveDomainModal';
import MoveGlossaryEntityModal from '@app/entity/shared/EntityDropdown/MoveGlossaryEntityModal';
import { UpdateDeprecationModal } from '@app/entity/shared/EntityDropdown/UpdateDeprecationModal';
import useDeleteEntity from '@app/entity/shared/EntityDropdown/useDeleteEntity';
import {
    canShowEditDeprecation,
    isDeleteDisabled,
    isMoveDisabled,
    shouldDisplayChildDeletionWarning,
} from '@app/entity/shared/EntityDropdown/utils';
import { getEntityPath } from '@app/entity/shared/containers/profile/utils';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import { AddIncidentModal } from '@app/entity/shared/tabs/Incident/components/AddIncidentModal';
import { getEntityProfileDeleteRedirectPath } from '@app/shared/deleteUtils';
import { useIsNestedDomainsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useUpdateDeprecationMutation } from '@graphql/mutations.generated';
import { Deprecation, EntityType } from '@types';

import DeprecatedIcon from '@images/deprecated-status.svg?react';

// Programmatic tab/path segment passed to getEntityPath — not user-visible copy.
const INCIDENTS_TAB_NAME = 'Incidents';

export enum EntityMenuItems {
    COPY_URL,
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
    color: ${(props) => props.theme.colors.text};
`;

const StyledMenuItem = styled(Menu.Item)<{ disabled?: boolean }>`
    &&&& {
        background-color: transparent;
    }
    ${(props) =>
        props.disabled
            ? `
            ${MenuItem} {
                color: ${props.theme.colors.textDisabled};
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
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
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
    const [deprecationModalInitialValues, setDeprecationModalInitialValues] = useState<Deprecation | null>(null);
    const [isMoveModalVisible, setIsMoveModalVisible] = useState(false);
    const [isRaiseIncidentModalVisible, setIsRaiseIncidentModalVisible] = useState(false);

    const handleUpdateDeprecation = async (deprecatedStatus: boolean) => {
        message.loading({ content: tf('updating') });
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
            message.success({
                content: deprecatedStatus
                    ? t('deprecation.markedDeprecatedSuccess')
                    : t('deprecation.markedUnDeprecatedSuccess'),
                duration: 2,
            });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({
                    content: t('deprecation.updateError', { errorMessage: e.message || '' }),
                    duration: 2,
                });
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

    type DropdownMenuItem = {
        key: string | number;
        label: React.ReactNode;
    };

    const deprecationItems: DropdownMenuItem[] = (() => {
        if (!menuItems.has(EntityMenuItems.UPDATE_DEPRECATION)) {
            return [];
        }

        if (!entityData?.deprecation?.deprecated) {
            return [
                {
                    key: 1,
                    label: (
                        <MenuItem
                            onClick={() => {
                                setDeprecationModalInitialValues(null);
                                setIsDeprecationModalVisible(true);
                            }}
                        >
                            <StyledDeprecatedIcon /> &nbsp; {t('deprecation.markDeprecated')}
                        </MenuItem>
                    ),
                },
            ];
        }

        const itemsForDeprecatedEntity: DropdownMenuItem[] = [];

        if (canShowEditDeprecation(entityData.privileges)) {
            itemsForDeprecatedEntity.push({
                key: '1-edit',
                label: (
                    <MenuItem
                        data-testid="entity-menu-edit-deprecation-button"
                        onClick={() => {
                            setDeprecationModalInitialValues(entityData.deprecation ?? null);
                            setIsDeprecationModalVisible(true);
                        }}
                    >
                        <StyledDeprecatedIcon /> &nbsp; {t('deprecation.editDeprecated')}
                    </MenuItem>
                ),
            });
        }

        itemsForDeprecatedEntity.push({
            key: '1-un',
            label: (
                <MenuItem onClick={() => handleUpdateDeprecation(false)}>
                    <StyledDeprecatedIcon /> &nbsp; {t('deprecation.markUnDeprecated')}
                </MenuItem>
            ),
        });

        return itemsForDeprecatedEntity;
    })();

    const items = [
        menuItems.has(EntityMenuItems.COPY_URL) && navigator.clipboard
            ? {
                  key: 0,
                  label: (
                      <StyledMenuItem
                          onClick={() => {
                              navigator.clipboard.writeText(pageUrl);
                              message.info(t('menuItem.copiedUrl'), 1.2);
                          }}
                      >
                          <MenuItem>
                              {/* eslint-disable-next-line jsx-a11y/anchor-is-valid */}
                              <Link /> &nbsp; {t('menuItem.copyUrl')}
                          </MenuItem>
                      </StyledMenuItem>
                  ),
              }
            : null,
        ...deprecationItems,
        menuItems.has(EntityMenuItems.ADD_TERM)
            ? {
                  key: 3,
                  label: (
                      <StyledMenuItem
                          data-testid="entity-menu-add-term-button"
                          key="2"
                          disabled={!canCreateGlossaryEntity}
                          onClick={() => setIsCreateTermModalVisible(true)}
                      >
                          <MenuItem>
                              <PlusCircle /> &nbsp;{t('menuItem.addTerm')}
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
                          disabled={!canCreateGlossaryEntity}
                          onClick={() => setIsCreateNodeModalVisible(true)}
                      >
                          <MenuItem>
                              <FolderPlus /> &nbsp;{t('menuItem.addTermGroup')}
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
                              <FolderOpen /> &nbsp;{tc('move')}
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
                                  // eslint-disable-next-line no-nested-ternary
                                  shouldDisplayChildDeletionWarning(entityType, entityData, me.platformPrivileges)
                                      ? isDomainEntity
                                          ? t('delete.cantDeleteSubDomain', {
                                                entityName: entityRegistry.getEntityName(entityType),
                                            })
                                          : t('delete.cantDeleteChild', {
                                                entityName: entityRegistry.getEntityName(entityType),
                                            })
                                      : undefined
                              }
                          >
                              <MenuItem data-testid="entity-menu-delete-button">
                                  <Trash /> &nbsp;{tc('delete')}
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
                              <CopySimple /> &nbsp;{t('menuItem.clone')}
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
                              <Warning /> &nbsp;{t('menuItem.raiseIncident')}
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
            {isCloneEntityModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={entityType}
                    onClose={() => setIsCloneEntityModalVisible(false)}
                    refetchData={entityType === EntityType.GlossaryTerm ? refetchForTerms : refetchForNodes}
                    isCloning
                />
            )}
            {isDeprecationModalVisible && (
                <UpdateDeprecationModal
                    urns={[urn]}
                    initialDeprecation={deprecationModalInitialValues}
                    onClose={() => {
                        setIsDeprecationModalVisible(false);
                        setDeprecationModalInitialValues(null);
                    }}
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
                                    INCIDENTS_TAB_NAME,
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
