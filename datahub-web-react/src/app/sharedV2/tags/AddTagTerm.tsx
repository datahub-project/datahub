import React from 'react';

import AddTagsModal from '@app/shared/tags/AddTagsModal';
import AddTermsModal from '@app/shared/tags/AddTermsModal';

import { EntityType, SubResourceType } from '@types';

type Props = {
    onOpenModal?: () => void;
    entityUrn?: string;
    entityType?: EntityType;
    entitySubresource?: string;
    refetch?: () => Promise<any>;
    showAddModal: boolean;
    setShowAddModal: React.Dispatch<React.SetStateAction<boolean>>;
    addModalType: EntityType | undefined;
    existingUrns?: string[];
};

export default function AddTagTerm({
    onOpenModal,
    entityUrn,
    entityType,
    entitySubresource,
    refetch,
    showAddModal,
    setShowAddModal,
    addModalType,
    existingUrns,
}: Props) {
    if (!showAddModal || !entityUrn || !entityType) return null;

    const onClose = () => {
        onOpenModal?.();
        setShowAddModal(false);
        setTimeout(() => refetch?.(), 2000);
    };
    const resources = [
        {
            resourceUrn: entityUrn,
            subResource: entitySubresource,
            subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
        },
    ];

    return addModalType === EntityType.Tag ? (
        <AddTagsModal open onCloseModal={onClose} resources={resources} existingUrns={existingUrns} />
    ) : (
        <AddTermsModal open onCloseModal={onClose} resources={resources} existingUrns={existingUrns} />
    );
}
