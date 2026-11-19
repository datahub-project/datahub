import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import { ActionDisabledReason } from '@app/entityV2/shared/components/styled/search/types';
import AddTagsModal from '@app/sharedV2/tags/AddTagsModal';
import { OperationType } from '@app/sharedV2/tags/useBatchTagTermMutation';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    disabledReason?: ActionDisabledReason;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function TagsDropdown({ urns, disabled = false, disabledReason, refetch }: Props) {
    const { t } = useTranslation('entity.shared.components');
    const { t: tcl } = useTranslation('common.labels');
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [operationType, setOperationType] = useState(OperationType.ADD);

    return (
        <>
            <ActionDropdown
                name={tcl('tags')}
                actions={[
                    {
                        title: t('searchActions.tags.addTitle'),
                        onClick: () => {
                            setOperationType(OperationType.ADD);
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('searchActions.tags.removeTitle'),
                        onClick: () => {
                            setOperationType(OperationType.REMOVE);
                            setIsEditModalVisible(true);
                        },
                    },
                ]}
                disabled={disabled}
                disabledReason={disabledReason}
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
