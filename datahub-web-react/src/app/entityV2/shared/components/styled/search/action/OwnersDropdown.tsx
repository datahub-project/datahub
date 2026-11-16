import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import {
    EditOwnersModal,
    OperationType,
} from '@app/entityV2/shared/containers/profile/sidebar/Ownership/EditOwnersModal';

type Props = {
    urns: Array<string>;
    disabledReason?: string;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function OwnersDropdown({ urns, disabledReason, refetch }: Props) {
    const { t } = useTranslation('entity.shared.components');
    const { t: tcl } = useTranslation('common.labels');
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [operationType, setOperationType] = useState(OperationType.ADD);

    return (
        <>
            <ActionDropdown
                name={tcl('owners')}
                actions={[
                    {
                        title: t('searchActions.owners.addTitle'),
                        onClick: () => {
                            setOperationType(OperationType.ADD);
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('searchActions.owners.removeTitle'),
                        onClick: () => {
                            setOperationType(OperationType.REMOVE);
                            setIsEditModalVisible(true);
                        },
                    },
                ]}
                disabledReason={disabledReason}
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
