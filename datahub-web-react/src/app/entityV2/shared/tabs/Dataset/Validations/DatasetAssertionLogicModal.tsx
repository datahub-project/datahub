import { Modal } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import Query from '@app/entityV2/shared/tabs/Dataset/Queries/Query';

type Props = {
    logic: string;
    visible: boolean;
    onClose: () => void;
};

export const DatasetAssertionLogicModal = ({ logic, visible, onClose }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tc } = useTranslation('common.actions');
    if (!visible) return null;
    return (
        <Modal
            title={t('assertionSummary.detailsLogicTitle')}
            onCancel={onClose}
            buttons={[{ text: tc('close'), variant: 'outline', color: 'gray', onClick: onClose }]}
        >
            <Query query={logic} />
        </Modal>
    );
};
