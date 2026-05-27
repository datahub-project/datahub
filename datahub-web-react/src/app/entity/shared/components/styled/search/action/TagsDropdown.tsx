import React, { useState } from 'react';

import ActionDropdown from '@app/entity/shared/components/styled/search/action/ActionDropdown';
import AddTagsModal from '@app/shared/tags/AddTagsModal';
import { OperationType } from '@app/shared/tags/useBatchTagTermMutation';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function TagsDropdown({ urns, disabled = false, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [operationType, setOperationType] = useState(OperationType.ADD);

    return (
        <>
            <ActionDropdown
                name="Tags"
                actions={[
                    {
                        title: 'Add tags',
                        onClick: () => {
                            setOperationType(OperationType.ADD);
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: 'Remove tags',
                        onClick: () => {
                            setOperationType(OperationType.REMOVE);
                            setIsEditModalVisible(true);
                        },
                    },
                ]}
                disabled={disabled}
            />
            {isEditModalVisible && (
                <AddTagsModal
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
