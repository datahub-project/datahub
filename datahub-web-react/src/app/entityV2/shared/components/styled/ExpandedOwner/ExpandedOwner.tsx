import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { getNameFromType } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import ActorPill from '@app/sharedV2/owners/ActorPill';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveOwnerMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, EntityType, Owner } from '@types';

const OwnerWrapper = styled.div``;

type Props = {
    entityUrn?: string;
    owner: Owner;
    refetch?: () => Promise<any>;
    readOnly?: boolean;
};

export const ExpandedOwner = ({ entityUrn, owner, refetch, readOnly }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { entityType } = useEntityData();
    const [removeOwnerMutation] = useRemoveOwnerMutation();
    const [showRemoveOwnerModal, setShowRemoveOwnerModal] = useState(false);
    const { reloadByKeyType } = useReloadableContext();
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
            if (isCurrentUserRemoved)
                reloadByKeyType(
                    [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.OwnedAssets)],
                    3000,
                );
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
        setShowRemoveOwnerModal(true);
    };

    const propagationDetails = { attribution: owner.attribution };

    return (
        <>
            <OwnerWrapper>
                <ActorPill
                    actor={owner.owner}
                    onClose={!readOnly ? onClose : undefined}
                    hideLink={readOnly}
                    propagationDetails={propagationDetails}
                />
            </OwnerWrapper>
            <ConfirmationModal
                isOpen={showRemoveOwnerModal}
                handleClose={() => setShowRemoveOwnerModal(false)}
                handleConfirm={onDelete}
                modalTitle={`Do you want to remove ${name}?`}
                modalText={`Are you sure you want to remove ${name} as an ${ownershipTypeName} type owner?`}
            />
        </>
    );
};
