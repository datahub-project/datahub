import { DatePicker, Modal, TextArea, toast } from '@components';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { handleBatchError } from '@app/entity/shared/utils';
import { decommissionTimeToSeconds } from '@app/shared/time/timeUtils';
import type { Dayjs } from '@utils/dayjs';
import dayjs from '@utils/dayjs';

import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';
import { Deprecation } from '@types';

type Props = {
    urns: string[];
    initialDeprecation?: Deprecation | null;
    onClose: () => void;
    refetch?: () => void;
};

const getInitialFormValues = (initialDeprecation?: Deprecation | null) => ({
    note: initialDeprecation?.note ?? '',
    decommissionTime: initialDeprecation?.decommissionTime
        ? dayjs.unix(decommissionTimeToSeconds(initialDeprecation.decommissionTime))
        : undefined,
});

const FieldGroup = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

export const UpdateDeprecationModal = ({ urns, initialDeprecation, onClose, refetch }: Props) => {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();
    const isEditMode = !!initialDeprecation;

    const initialFormValues = getInitialFormValues(initialDeprecation);
    const [note, setNote] = useState<string>(initialFormValues.note);
    const [decommissionTime, setDecommissionTime] = useState<Dayjs | null | undefined>(
        initialFormValues.decommissionTime,
    );

    useEffect(() => {
        const nextValues = getInitialFormValues(initialDeprecation);
        setNote(nextValues.note);
        setDecommissionTime(nextValues.decommissionTime);
    }, [initialDeprecation]);

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
            toast.success(isEditMode ? t('deprecation.updated') : t('deprecation.markedDeprecatedSuccess'), {
                duration: 2,
            });
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
            title={isEditMode ? t('deprecation.editTitle') : t('deprecation.addDetailsTitle')}
            onCancel={onClose}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    buttonDataTestId: 'add-deprecation-submit',
                    text: isEditMode ? tc('save') : t('deprecation.ok'),
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
                    key={initialDeprecation?.decommissionTime ?? 'new-deprecation'}
                    placeholder={t('deprecation.decommissionDateLabel')}
                    value={decommissionTime}
                    onChange={(v) => setDecommissionTime(v)}
                />
            </FieldGroup>
        </Modal>
    );
};
