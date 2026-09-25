import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { LogicalModelColumnDraft } from '@app/entityV2/shared/logicalModels/logicalModels.types';
import {
    LOGICAL_MODEL_COLUMN_TYPE_OPTIONS,
    columnsFromSchemaOrThrow,
} from '@app/entityV2/shared/logicalModels/logicalModels.utils';
import { Input, Modal, SimpleSelect, toast } from '@src/alchemy-components';

import { useGetLogicalModelColumnsLazyQuery, useUpdateLogicalModelSchemaMutation } from '@graphql/logical.generated';
import { SchemaFieldDataType } from '@types';

const Field = styled.div`
    margin-bottom: 16px;
`;

const Warning = styled.div`
    color: ${(props) => props.theme.colors.textWarning};
    font-size: 12px;
`;

type Props = {
    datasetUrn: string;
    fieldPath: string;
    currentType: SchemaFieldDataType;
    childCount: number;
    onClose: () => void;
    onUpdated?: () => void;
};

export default function EditLogicalModelColumnModal({
    datasetUrn,
    fieldPath,
    currentType,
    childCount,
    onClose,
    onUpdated,
}: Props) {
    const { t } = useTranslation('logicalModels');
    const [name, setName] = useState(fieldPath);
    const [type, setType] = useState<SchemaFieldDataType>(currentType);
    const [submitting, setSubmitting] = useState(false);
    const [fetchColumns] = useGetLogicalModelColumnsLazyQuery({ fetchPolicy: 'network-only' });
    const [updateSchema] = useUpdateLogicalModelSchemaMutation();

    const changed = name.trim() !== fieldPath || type !== currentType;
    const canSave = !!name.trim() && changed;

    const onSubmit = async () => {
        const newPath = name.trim();
        setSubmitting(true);
        let next: LogicalModelColumnDraft[];
        try {
            const { data } = await fetchColumns({ variables: { urn: datasetUrn } });
            next = columnsFromSchemaOrThrow(data?.dataset?.schemaMetadata?.fields).map((col) =>
                col.fieldPath === fieldPath ? { fieldPath: newPath, type } : col,
            );
        } catch {
            toast.error(t('schema.readErrorMessage'), { duration: 3 });
            setSubmitting(false);
            return;
        }
        const paths = next.map((col) => col.fieldPath);
        if (new Set(paths).size !== paths.length) {
            toast.error(t('column.duplicateError', { name: newPath }), { duration: 3 });
            setSubmitting(false);
            return;
        }
        updateSchema({ variables: { input: { urn: datasetUrn, columns: next } } })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('schema.successMessage'), { duration: 2 });
                    onUpdated?.();
                }
            })
            .catch((e) => toast.error(t('schema.errorMessage', { error: e.message }), { duration: 3 }))
            .finally(() => {
                setSubmitting(false);
                onClose();
            });
    };

    return (
        <Modal
            title={t('editColumn.title')}
            onCancel={onClose}
            buttons={[
                { text: t('modal.cancel'), variant: 'text', onClick: onClose },
                {
                    text: t('column.save'),
                    variant: 'filled',
                    buttonDataTestId: 'submit-edit-logical-model-column',
                    disabled: !canSave || submitting,
                    isLoading: submitting,
                    onClick: onSubmit,
                },
            ]}
        >
            <Field>
                <Input label={t('column.nameLabel')} value={name} setValue={setName} isRequired />
            </Field>
            <Field>
                <div>{t('column.typeLabel')}</div>
                <SimpleSelect
                    width={200}
                    values={[type]}
                    showClear={false}
                    onUpdate={(vals) => vals[0] && setType(vals[0] as SchemaFieldDataType)}
                    options={LOGICAL_MODEL_COLUMN_TYPE_OPTIONS.map((opt) => ({ value: opt, label: opt }))}
                />
            </Field>
            {childCount > 0 && <Warning>{t('editColumn.unlinkWarning', { count: childCount })}</Warning>}
        </Modal>
    );
}
