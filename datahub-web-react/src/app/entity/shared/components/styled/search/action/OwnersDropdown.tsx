import React, { useState } from 'react';
import { EditOwnersModal, OperationType } from '../../../../containers/profile/sidebar/Ownership/EditOwnersModal';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function OwnersDropdown({ urns, disabled = false, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [operationType, setOperationType] = useState(OperationType.ADD);

    return (
        <>
            <ActionDropdown
                name="Owners"
                actions={[
                    {
                        title: 'Add owners',
                        onClick: () => {
                            setOperationType(OperationType.ADD);
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: 'Remove owners',
                        onClick: () => {
                            setOperationType(OperationType.REMOVE);
                            setIsEditModalVisible(true);
                        },
                    },
                ]}
                disabled={disabled}
            />
            {isEditModalVisible && (
                <EditOwnersModal
                    urns={urns}
                    operationType={operationType}
                    onCloseModal={() => {
                        setIsEditModalVisible(false);
                        refetch?.();
                    }}
                    hideOwnerType={operationType === OperationType.REMOVE}
                />
            )}
        </>
    );
}
