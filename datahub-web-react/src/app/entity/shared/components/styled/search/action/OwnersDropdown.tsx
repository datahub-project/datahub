import React, { useState } from 'react';
import { EntityType } from '../../../../../../../types.generated';
import { AddOwnersModal, OperationType } from '../../../../containers/profile/sidebar/Ownership/AddOwnersModal';
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
                <AddOwnersModal
                    urns={urns}
                    type={EntityType.Dataset} // TODO: Remove
                    operationType={operationType}
                    onCloseModal={() => {
                        setIsEditModalVisible(true);
                        refetch?.();
                    }}
                />
            )}
        </>
    );
}
