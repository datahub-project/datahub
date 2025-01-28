import { message, Modal, Tag } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { StyledLink } from '@src/app/previewV2/EntityHeader';
import { useRemoveOwnerMutation } from '../../../../../../graphql/mutations.generated';
import { EntityType, Owner } from '../../../../../../types.generated';
import { getNameFromType } from '../../../containers/profile/sidebar/Ownership/ownershipUtils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import OwnerContent from './OwnerContent';
import { useEmbeddedProfileLinkProps } from '../../../../../shared/useEmbeddedProfileLinkProps';

const OwnerTag = styled(Tag)`
    padding: 1px;
    padding-right: 6px;
    margin-bottom: 8px;
    display: inline-flex;
    align-items: center;
    font-weight: 600;
    border-color: #9da7c0 !important;
    padding: 2px 6px 2px 3px;

    max-width: inherit;
`;

type Props = {
    entityUrn?: string;
    owner: Owner;
    hidePopOver?: boolean | undefined;
    refetch?: () => Promise<any>;
    readOnly?: boolean;
    fontSize?: number;
};

export const ExpandedOwner = ({ entityUrn, owner, hidePopOver, refetch, readOnly, fontSize }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { entityType } = useEntityData();
    const linkProps = useEmbeddedProfileLinkProps();
    const [removeOwnerMutation] = useRemoveOwnerMutation();
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

    return (
        <OwnerTag onClose={onClose} closable={!!entityUrn && !readOnly}>
            {readOnly && <OwnerContent name={name} owner={owner} hidePopOver={hidePopOver} pictureLink={pictureLink} />}
            {!readOnly && (
                <StyledLink
                    to={`${entityRegistry.getEntityUrl(owner.owner.type, owner.owner.urn)}/owner of`}
                    {...linkProps}
                >
                    <OwnerContent
                        name={name}
                        owner={owner}
                        hidePopOver={hidePopOver}
                        pictureLink={pictureLink}
                        fontSize={fontSize}
                    />
                </StyledLink>
            )}
        </OwnerTag>
    );
};
