import { Modal, message } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { getNameFromType } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';
import { useModulesContext } from '@app/homeV3/module/context/ModulesContext';
import OwnerPill from '@app/sharedV2/owners/components/OwnerPill';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveOwnerMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, EntityType, Owner } from '@types';

const OwnerWrapper = styled.div``;

type Props = {
    entityUrn?: string;
    owner: Owner;
    refetch?: () => Promise<any>;
    readOnly?: boolean;
    hidePopOver?: boolean;
};

export const ExpandedOwner = ({ entityUrn, owner, refetch, readOnly, hidePopOver }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { entityType } = useEntityData();
    const [removeOwnerMutation] = useRemoveOwnerMutation();
    const { reloadModules } = useModulesContext();
    const { user } = useUserContext();

    let name = '';
    let ownershipTypeName = '';
    if (owner.owner.__typename === 'CorpGroup') {
        name = entityRegistry.getDisplayName(EntityType.CorpGroup, owner.owner);
    }
    if (owner.owner.__typename === 'CorpUser') {
        name = entityRegistry.getDisplayName(EntityType.CorpUser, owner.owner);
    }
    if (owner.ownershipType && owner.ownershipType.info) {
        ownershipTypeName = owner.ownershipType.info.name;
    } else if (owner.type) {
        ownershipTypeName = getNameFromType(owner.type);
    }

    const onDelete = async () => {
        if (!entityUrn) {
            return;
        }
        try {
            await removeOwnerMutation({
                variables: {
                    input: {
                        ownerUrn: owner.owner.urn,
                        ownershipTypeUrn: owner.ownershipType?.urn,
                        resourceUrn: entityUrn,
                    },
                },
            });
            message.success({ content: 'Owner Removed', duration: 2 });
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateOwnership,
                entityType,
                entityUrn,
            });
            const isCurrentUserRemoved = user?.urn === owner.owner.urn;
            // Reload modules
            // OwnedAssets - update Your assets module on home page
            if (isCurrentUserRemoved) reloadModules([DataHubPageModuleType.OwnedAssets], 3000);
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove owner: \n ${e.message || ''}`, duration: 3 });
            }
        }
        refetch?.();
    };
    const onClose = (e) => {
        e.preventDefault();
        Modal.confirm({
            title: `Do you want to remove ${name}?`,
            content: `Are you sure you want to remove ${name} as an ${ownershipTypeName} type owner?`,
            onOk() {
                onDelete();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const propagationDetails = { attribution: owner.attribution };

    return (
        <>
            <OwnerWrapper>
                <OwnerPill
                    owner={owner.owner}
                    onRemove={!readOnly ? onClose : undefined}
                    hideLink={readOnly}
                    hidePopOver={hidePopOver}
                    propagationDetails={propagationDetails}
                />
            </OwnerWrapper>
        </>
    );
};
