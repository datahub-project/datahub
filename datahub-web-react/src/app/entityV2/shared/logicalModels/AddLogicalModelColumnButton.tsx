import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { LogicalModelColumnDraft } from '@app/entityV2/shared/logicalModels/logicalModels.types';
import {
    LOGICAL_MODEL_COLUMN_TYPE_OPTIONS,
    columnsFromSchemaOrThrow,
} from '@app/entityV2/shared/logicalModels/logicalModels.utils';
import { Button, Input, Modal, SimpleSelect, Tooltip, toast } from '@src/alchemy-components';

import { useGetLogicalModelColumnsLazyQuery, useUpdateLogicalModelSchemaMutation } from '@graphql/logical.generated';
import { SchemaFieldDataType } from '@types';

const Row = styled.div`
    display: flex;
    gap: 16px;
    align-items: flex-end;

    > * {
        flex: 1;
        min-width: 0;
    }
`;

export default function AddLogicalModelColumnButton() {
    const { t } = useTranslation('logicalModels');
    const { urn } = useEntityData();
    const refetch = useRefetch();
    const [open, setOpen] = useState(false);
    const [name, setName] = useState('');
    const [type, setType] = useState<SchemaFieldDataType>(SchemaFieldDataType.String);
    const [submitting, setSubmitting] = useState(false);
    const [fetchColumns] = useGetLogicalModelColumnsLazyQuery({ fetchPolicy: 'network-only' });
    const [updateSchema] = useUpdateLogicalModelSchemaMutation();

    const canAdd = !!name.trim();

    const onOpen = () => {
        setName('');
        setType(SchemaFieldDataType.String);
        setOpen(true);
    };

    const onSubmit = async () => {
        const fieldPath = name.trim();
        setSubmitting(true);
        let existing: LogicalModelColumnDraft[];
        try {
            const { data } = await fetchColumns({ variables: { urn } });
            existing = columnsFromSchemaOrThrow(data?.dataset?.schemaMetadata?.fields);
        } catch {
            toast.error(t('schema.readErrorMessage'), { duration: 3 });
            setSubmitting(false);
            return;
        }
        if (existing.some((col) => col.fieldPath === fieldPath)) {
            toast.error(t('column.duplicateError', { name: fieldPath }), { duration: 3 });
            setSubmitting(false);
            return;
        }
        const next = [...existing, { fieldPath, type }];
        updateSchema({ variables: { input: { urn, columns: next } } })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('schema.successMessage'), { duration: 2 });
                    refetch();
                }
            })
            .catch((e) => toast.error(t('schema.errorMessage', { error: e.message }), { duration: 3 }))
            .finally(() => {
                setSubmitting(false);
                setOpen(false);
            });
    };

    return (
        <>
            <Tooltip title={t('addColumn.tooltip')} showArrow={false}>
                <Button
                    variant="text"
                    color="gray"
                    icon={{ icon: Plus }}
                    onClick={onOpen}
                    data-testid="add-logical-model-column-button"
                />
            </Tooltip>
            {open && (
                <Modal
                    title={t('addColumn.title')}
                    onCancel={() => setOpen(false)}
                    buttons={[
                        { text: t('modal.cancel'), variant: 'text', onClick: () => setOpen(false) },
                        {
                            text: t('column.add'),
                            variant: 'filled',
                            buttonDataTestId: 'submit-add-logical-model-column',
                            disabled: !canAdd || submitting,
                            isLoading: submitting,
                            onClick: onSubmit,
                        },
                    ]}
                >
                    <Row>
                        <Input label={t('column.nameLabel')} value={name} setValue={setName} isRequired />
                        <SimpleSelect
                            label={t('column.typeLabel')}
                            width="full"
                            values={[type]}
                            showClear={false}
                            onUpdate={(vals) => vals[0] && setType(vals[0] as SchemaFieldDataType)}
                            options={LOGICAL_MODEL_COLUMN_TYPE_OPTIONS.map((opt) => ({ value: opt, label: opt }))}
                        />
                    </Row>
                </Modal>
            )}
        </>
    );
}
