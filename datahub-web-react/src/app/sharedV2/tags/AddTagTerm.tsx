import React from 'react';

import { EntityType, SubResourceType } from '../../../types.generated';

import EditTagTermsModal from '../../shared/tags/AddTagsTermsModal';

type Props = {
    onOpenModal?: () => void;
    entityUrn?: string;
    entityType?: EntityType;
    entitySubresource?: string;
    refetch?: () => Promise<any>;
    showAddModal: boolean;
    setShowAddModal: React.Dispatch<React.SetStateAction<boolean>>;
    addModalType: any;
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
}: Props) {
    return (
        <>
            {showAddModal && !!entityUrn && !!entityType && (
                <EditTagTermsModal
                    type={addModalType}
                    open
                    onCloseModal={() => {
                        onOpenModal?.();
                        setShowAddModal(false);
                        setTimeout(() => refetch?.(), 2000);
                    }}
                    resources={[
                        {
                            resourceUrn: entityUrn,
                            subResource: entitySubresource,
                            subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                        },
                    ]}
                />
            )}
        </>
    );
}
