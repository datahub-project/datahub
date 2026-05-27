import React, { useState } from 'react';

import ActionDropdown from '@app/entity/shared/components/styled/search/action/ActionDropdown';
import AddTermsModal from '@app/shared/tags/AddTermsModal';
import { OperationType } from '@app/shared/tags/useBatchTagTermMutation';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function GlossaryTermsDropdown({ urns, disabled = false, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [operationType, setOperationType] = useState(OperationType.ADD);

    return (
        <>
            <ActionDropdown
                name="Glossary Terms"
                actions={[
                    {
                        title: 'Add Glossary Terms',
                        onClick: () => {
                            setOperationType(OperationType.ADD);
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: 'Remove Glossary Terms',
                        onClick: () => {
                            setOperationType(OperationType.REMOVE);
                            setIsEditModalVisible(true);
                        },
                    },
                ]}
                disabled={disabled}
            />
            {isEditModalVisible && (
                <AddTermsModal
                    open
                    onCloseModal={() => {
                        setIsEditModalVisible(false);
                        refetch?.();
                    }}
                    resources={urns.map((urn) => ({ resourceUrn: urn }))}
                    operationType={operationType}
                />
            )}
        </>
    );
}
