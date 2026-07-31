import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entity/shared/components/styled/search/action/ActionDropdown';
import {
    EditOwnersModal,
    OperationType,
} from '@app/entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function OwnersDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const { t: tl } = useTranslation('common.labels');
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [operationType, setOperationType] = useState(OperationType.ADD);

    return (
        <>
            <ActionDropdown
                name={tl('owners')}
                actions={[
                    {
                        title: t('action.owners.add'),
                        onClick: () => {
                            setOperationType(OperationType.ADD);
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('action.owners.remove'),
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
