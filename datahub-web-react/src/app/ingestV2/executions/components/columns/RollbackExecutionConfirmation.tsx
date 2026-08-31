import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

interface Props {
    isOpen: boolean;
    onConfirm: () => void;
    onCancel: () => void;
}

export default function RollbackExecutionConfirmation({ isOpen, onConfirm, onCancel }: Props) {
    const { t } = useTranslation('ingestion');
    return (
        <ConfirmationModal
            isOpen={isOpen}
            modalTitle={t('executions.rollbackConfirmTitle')}
            modalText={
                <Trans
                    i18nKey="executions.rollbackConfirmText"
                    t={t}
                    components={{
                        anchor: (
                            // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                            <a
                                href="https://docs.datahub.com/docs/how/delete-metadata#rollback-ingestion-run"
                                target="_blank"
                                rel="noopener noreferrer"
                            />
                        ),
                    }}
                />
            }
            handleConfirm={onConfirm}
            handleClose={onCancel}
        />
    );
}
