import { message, Modal, Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useRemoveOwnerMutation } from '../../../../../../graphql/mutations.generated';
import { EntityType, Owner } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { useEntityData } from '../../../EntityContext';
import OwnerContent from './OwnerContent';

const OwnerTag = styled(Tag)`
    padding: 2px;
    padding-right: 6px;
    margin-bottom: 8px;
    display: inline-flex;
    align-items: center;
`;

type Props = {
    entityUrn?: string;
    owner: Owner;
    hidePopOver?: boolean | undefined;
    refetch?: () => Promise<any>;
    readOnly?: boolean;
};

export const ExpandedOwner = ({ entityUrn, owner, hidePopOver, refetch, readOnly }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { entityType } = useEntityData();
    const [removeOwnerMutation] = useRemoveOwnerMutation();
    let name = '';
    if (owner.owner.__typename === 'CorpGroup') {
        name = entityRegistry.getDisplayName(EntityType.CorpGroup, owner.owner);
    }
    if (owner.owner.__typename === 'CorpUser') {
        name = entityRegistry.getDisplayName(EntityType.CorpUser, owner.owner);
    }
    const pictureLink =
        (owner.owner.__typename === 'CorpUser' && owner.owner.editableProperties?.pictureLink) || undefined;
    const onDelete = async () => {
        if (!entityUrn) {
            return;
        }
        try {
            await removeOwnerMutation({
                variables: {
                    input: {
                        ownerUrn: owner.owner.urn,
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
            content: `Are you sure you want to remove ${name} as an owner?`,
            onOk() {
                onDelete();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <OwnerTag onClose={onClose} closable={!!entityUrn && !readOnly}>
            {readOnly && <OwnerContent name={name} owner={owner} hidePopOver={hidePopOver} pictureLink={pictureLink} />}
            {!readOnly && (
                <Link to={entityRegistry.getEntityUrl(owner.owner.type, owner.owner.urn)}>
                    <OwnerContent name={name} owner={owner} hidePopOver={hidePopOver} pictureLink={pictureLink} />
                </Link>
            )}
        </OwnerTag>
    );
};
