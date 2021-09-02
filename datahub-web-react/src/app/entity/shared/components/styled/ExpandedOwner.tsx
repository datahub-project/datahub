import { Modal, Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { Maybe, Owner, OwnershipUpdate } from '../../../../../types.generated';
import { CustomAvatar } from '../../../../shared/avatar';
import { useEntityRegistry } from '../../../../useEntityRegistry';

type Props = {
    owner: Owner;
    owners: Maybe<Owner[]> | undefined;
    updateOwnership: (update: OwnershipUpdate) => void;
};

const OwnerTag = styled(Tag)`
    padding: 2px;
    padding-right: 6px;
    margin-bottom: 8px;
    display: inline-flex;
    align-items: center;
`;

export const ExpandedOwner = ({ owner, updateOwnership, owners }: Props) => {
    const entityRegistry = useEntityRegistry();

    let name = '';
    if (owner.owner.__typename === 'CorpGroup') {
        name = owner.owner.name || owner.owner.info?.displayName || '';
    }
    if (owner.owner.__typename === 'CorpUser') {
        name = owner.owner.info?.displayName || owner.owner.info?.fullName || owner.owner.info?.email || '';
    }

    const pictureLink = (owner.owner.__typename === 'CorpUser' && owner.owner.editableInfo?.pictureLink) || undefined;

    const onDelete = () => {
        if (updateOwnership) {
            const updatedOwners =
                owners
                    ?.filter((someOwner) => !(someOwner.owner.urn === owner.owner.urn && someOwner.type === owner.type))
                    ?.map((someOwner) => ({
                        owner: someOwner.owner.urn,
                        type: someOwner.type,
                    })) || [];

            updateOwnership({ owners: updatedOwners });
        }
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
