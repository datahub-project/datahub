import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entity/shared/components/styled/search/action/ActionDropdown';
import AddTermsModal from '@app/shared/tags/AddTermsModal';
import { OperationType } from '@app/shared/tags/useBatchTagTermMutation';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function GlossaryTermsDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [operationType, setOperationType] = useState(OperationType.ADD);

    return (
        <>
            <ActionDropdown
                name={t('action.glossaryTerms.name')}
                actions={[
                    {
                        title: t('action.glossaryTerms.add'),
                        onClick: () => {
                            setOperationType(OperationType.ADD);
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('action.glossaryTerms.remove'),
                        onClick: () => {
                            setOperationType(OperationType.REMOVE);
                            setIsEditModalVisible(true);
                        },
                    },
                ]}
                disabled={disabled}
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
