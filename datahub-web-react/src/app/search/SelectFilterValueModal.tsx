import React from 'react';
import { EditOwnersModal } from '../entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';

type Props = {
    filterField: string;
    onSelect: (values: string[]) => void;
    onCloseModal: () => void;
    initialUrns?: string[];
};

export const SelectFilterValueModal = ({ filterField, onSelect, onCloseModal, initialUrns }: Props) => {
    if (filterField === 'owners') {
        return (
            <EditOwnersModal
                title="Select Owners"
                urns={[]}
                initialUrns={initialUrns}
                onCloseModal={onCloseModal}
                hideOwnerType
                onOkOverride={(owners) => {
                    onSelect(owners.map((owner) => owner.value.ownerUrn));
                    onCloseModal();
                }}
            />
        );
    }
    return null;
};
