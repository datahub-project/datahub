import React from 'react';

import EditTagTermsModal from '@app/shared/tags/AddTagsTermsModal';
import { shouldShowProposeButton } from '@app/shared/tags/utils/proposalUtils';

import { EntityType, SubResourceType } from '@types';

type Props = {
    onOpenModal?: () => void;
    entityUrn?: string;
    entityType?: EntityType;
    entitySubresource?: string;
    refetch?: () => Promise<any>;
    showAddModal: boolean;
    setShowAddModal: React.Dispatch<React.SetStateAction<boolean>>;
    addModalType: any;
    canAddTerm?: boolean;
    canProposeTerm?: boolean;
    canAddTag?: boolean;
    canProposeTag?: boolean;
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
    canAddTerm = true,
    canProposeTerm = true,
    canAddTag = true,
    canProposeTag = true,
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
                        setTimeout(() => refetch?.(), 3500);
                    }}
                    resources={[
                        {
                            resourceUrn: entityUrn,
                            subResource: entitySubresource,
                            subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                        },
                    ]}
                    showPropose={shouldShowProposeButton(entityType)}
                    canAddTerm={canAddTerm}
                    canProposeTerm={canProposeTerm}
                    canAddTag={canAddTag}
                    canProposeTag={canProposeTag}
                />
            )}
        </>
    );
}
