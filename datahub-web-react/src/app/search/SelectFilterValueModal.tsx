import React from 'react';
import { EditOwnersModal } from '../entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';

type Props = {
    filterField: string;
    onSelect: (values: string[]) => void;
    onCloseModal: () => void;
};

export const SelectFilterValueModal = ({ filterField, onSelect, onCloseModal }: Props) => {
    if (filterField === 'owners') {
        return (
            <EditOwnersModal
                title="Select Owners"
                urns={[]}
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
