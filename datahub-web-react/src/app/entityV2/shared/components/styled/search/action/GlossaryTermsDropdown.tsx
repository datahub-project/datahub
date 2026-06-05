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
export default function GlossaryTermsDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation('entity.shared.components');
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [operationType, setOperationType] = useState(OperationType.ADD);

    return (
        <>
            <ActionDropdown
                name={t('searchActions.glossaryTerms.name')}
                actions={[
                    {
                        title: t('searchActions.glossaryTerms.addTitle'),
                        onClick: () => {
                            setOperationType(OperationType.ADD);
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('searchActions.glossaryTerms.removeTitle'),
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
                    type={EntityType.GlossaryTerm}
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
