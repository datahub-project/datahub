import React from 'react';
import { useTranslation } from 'react-i18next';

import { LogicalModelColumnDraft } from '@app/entityV2/shared/logicalModels/logicalModels.types';
import { columnsFromSchemaOrThrow } from '@app/entityV2/shared/logicalModels/logicalModels.utils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { toast } from '@src/alchemy-components';

import { useGetLogicalModelColumnsLazyQuery, useUpdateLogicalModelSchemaMutation } from '@graphql/logical.generated';

type Props = {
    datasetUrn: string;
    fieldPath: string;
    childCount: number;
    open: boolean;
    onClose: () => void;
    onDeleted?: () => void;
};

export default function DeleteLogicalModelColumnButton({
    datasetUrn,
    fieldPath,
    childCount,
    open,
    onClose,
    onDeleted,
}: Props) {
    const { t } = useTranslation('logicalModels');
    const [fetchColumns] = useGetLogicalModelColumnsLazyQuery({ fetchPolicy: 'network-only' });
    const [updateSchema] = useUpdateLogicalModelSchemaMutation();

    const handleConfirm = async () => {
        // Close immediately so the confirm can't be double-fired while the request is in flight.
        onClose();
        let next: LogicalModelColumnDraft[];
        try {
            const { data } = await fetchColumns({ variables: { urn: datasetUrn } });
            next = columnsFromSchemaOrThrow(data?.dataset?.schemaMetadata?.fields).filter(
                (col) => col.fieldPath !== fieldPath,
            );
        } catch {
            toast.error(t('schema.readErrorMessage'), { duration: 3 });
            return;
        }
        updateSchema({ variables: { input: { urn: datasetUrn, columns: next } } })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('schema.successMessage'), { duration: 2 });
                    onDeleted?.();
                }
            })
            .catch((e) => toast.error(t('schema.errorMessage', { error: e.message }), { duration: 3 }));
    };

    const modalText =
        childCount > 0
            ? t('deleteColumn.confirm', { name: fieldPath, count: childCount })
            : t('deleteColumn.confirmNoChildren', { name: fieldPath });

    return (
        <ConfirmationModal
            isOpen={open}
            modalTitle={t('deleteColumn.menuLabel')}
            modalText={modalText}
            confirmButtonText={t('deleteColumn.okText')}
            handleConfirm={handleConfirm}
            handleClose={onClose}
        />
    );
}
