import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import { ActionDisabledReason } from '@app/entityV2/shared/components/styled/search/types';
import AddTermsModal from '@app/sharedV2/tags/AddTermsModal';
import { OperationType } from '@app/sharedV2/tags/useBatchTagTermMutation';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    disabledReason?: ActionDisabledReason;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function GlossaryTermsDropdown({ urns, disabled = false, disabledReason, refetch }: Props) {
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
                disabledReason={disabledReason}
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
