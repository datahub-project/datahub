import { DatePicker, Modal, TextArea, toast } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { handleBatchError } from '@app/entity/shared/utils';
import type { Dayjs } from '@utils/dayjs';

import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';

type Props = {
    urns: string[];
    onClose: () => void;
    refetch?: () => void;
};

const FieldGroup = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

export const UpdateDeprecationModal = ({ urns, onClose, refetch }: Props) => {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();

    const [note, setNote] = useState<string>('');
    const [decommissionTime, setDecommissionTime] = useState<Dayjs | null | undefined>(undefined);

    const handleSubmit = async () => {
        toast.loading(tf('updating'));
        try {
            await batchUpdateDeprecation({
                variables: {
                    input: {
                        resources: urns.map((urn) => ({ resourceUrn: urn })),
                        deprecated: true,
                        note,
                        decommissionTime: decommissionTime ? decommissionTime.unix() * 1000 : null,
                    },
                },
            });
            toast.destroy();
            toast.success(t('deprecation.markedDeprecatedSuccess'), { duration: 2 });
        } catch (e: unknown) {
            toast.destroy();
            if (e instanceof Error) {
                const fallback = {
                    content: t('deprecation.updateError', { errorMessage: e.message || '' }),
                    duration: 2,
                };
                const { content, duration } = handleBatchError(urns, e, fallback);
                toast.error(content, { duration });
            }
        }
        refetch?.();
        onClose();
    };

    return (
        <Modal
            title={t('deprecation.addDetailsTitle')}
            onCancel={onClose}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    buttonDataTestId: 'add-deprecation-submit',
                    text: t('deprecation.ok'),
                    onClick: handleSubmit,
                },
            ]}
        >
            <FieldGroup>
                <TextArea
                    label={t('deprecation.noteLabel')}
                    value={note}
                    onChange={(e) => setNote(e.target.value)}
                    rows={4}
                    autoFocus
                />
                <DatePicker
                    placeholder={t('deprecation.decommissionDateLabel')}
                    value={decommissionTime}
                    onChange={(v) => setDecommissionTime(v)}
                />
            </FieldGroup>
        </Modal>
    );
};
