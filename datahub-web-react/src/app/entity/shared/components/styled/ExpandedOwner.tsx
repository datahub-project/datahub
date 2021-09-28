import { message, Modal, Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useRemoveOwnerMutation } from '../../../../../graphql/mutations.generated';

import { Owner } from '../../../../../types.generated';
import { CustomAvatar } from '../../../../shared/avatar';
import { useEntityRegistry } from '../../../../useEntityRegistry';

type Props = {
    entityUrn: string;
    owner: Owner;
    refetch?: () => Promise<any>;
};

const OwnerTag = styled(Tag)`
    padding: 2px;
    padding-right: 6px;
    margin-bottom: 8px;
    display: inline-flex;
    align-items: center;
`;

export const ExpandedOwner = ({ entityUrn, owner, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [removeOwnerMutation] = useRemoveOwnerMutation();

    let name = '';
    if (owner.owner.__typename === 'CorpGroup') {
        name = owner.owner.name || owner.owner.info?.displayName || '';
    }
    if (owner.owner.__typename === 'CorpUser') {
        name = owner.owner.info?.displayName || owner.owner.info?.fullName || owner.owner.info?.email || '';
    }

    const pictureLink = (owner.owner.__typename === 'CorpUser' && owner.owner.editableInfo?.pictureLink) || undefined;

    const onDelete = async () => {
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
        <OwnerTag onClose={onClose} closable>
            <Link to={`/${entityRegistry.getPathName(owner.owner.type)}/${owner.owner.urn}`}>
                <CustomAvatar name={name} photoUrl={pictureLink} />
                {name}
            </Link>
        </OwnerTag>
    );
};
