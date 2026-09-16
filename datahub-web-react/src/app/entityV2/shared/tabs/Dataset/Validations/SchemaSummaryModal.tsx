import { Modal } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { SchemaSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/SchemaSummary';

import { SchemaMetadata } from '@types';

const modalStyle = {
    top: 40,
};

const modalBodyStyle = {
    padding: 0,
};

type Props = {
    schema: SchemaMetadata;
    onClose: () => void;
};

export const SchemaSummaryModal = ({ schema, onClose }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tc } = useTranslation('common.actions');
    return (
        <Modal
            width={800}
            style={modalStyle}
            bodyStyle={modalBodyStyle}
            title={t('schemaSummaryModal.title')}
            open
            onCancel={onClose}
            buttons={[
                {
                    text: tc('close'),
                    variant: 'filled',
                    onClick: onClose,
                },
            ]}
        >
            <SchemaSummary schema={schema} />
        </Modal>
    );
};
