import React, { useState } from 'react';

import ActionDropdown from '@app/entity/shared/components/styled/search/action/ActionDropdown';
import {
    BulkAddOrganizationModal,
    OperationType,
} from '@app/entity/shared/components/styled/search/action/BulkAddOrganizationModal';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function OrganizationsDropdown({ urns, disabled = false, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [operationType, setOperationType] = useState(OperationType.ADD);

    return (
        <>
            <ActionDropdown
                name="Organizations"
                actions={[
                    {
                        title: 'Add organizations',
                        onClick: () => {
                            setOperationType(OperationType.ADD);
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: 'Remove organizations',
                        onClick: () => {
                            setOperationType(OperationType.REMOVE);
                            setIsEditModalVisible(true);
                        },
                    },
                ]}
                disabled={disabled}
            />
            {isEditModalVisible && (
                <BulkAddOrganizationModal
                    open
                    onCloseModal={() => {
                        setIsEditModalVisible(false);
                        refetch?.();
                    }}
                    urns={urns}
                    operationType={operationType}
                />
            )}
        </>
    );
}
