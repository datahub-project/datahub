import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import EditTagTermsModal, { OperationType } from '@app/shared/tags/AddTagsTermsModal';

import { EntityType } from '@types';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function TagsDropdown({ urns, disabled = false, refetch }: Props) {
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
            />
            {isEditModalVisible && (
                <EditTagTermsModal
                    type={EntityType.Tag}
                    open
                    onCloseModal={() => {
                        setIsEditModalVisible(false);
                        refetch?.();
                    }}
                    resources={urns.map((urn) => ({
                        resourceUrn: urn,
                    }))}
                    operationType={operationType}
                />
            )}
        </>
    );
}
