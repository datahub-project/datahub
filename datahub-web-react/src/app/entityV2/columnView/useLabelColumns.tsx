import { CheckOutlined } from '@ant-design/icons';
import { ColumnsType } from 'antd/es/table';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { ColumnLike, columnHeader, columnIdentity, labelUrnOf } from '@app/entityV2/columnView/columnKinds';
import { EditableFieldInfo, editableFieldInfoByPath, fieldLabelUrns } from '@app/entityV2/columnView/filterSchemaRowsByView';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';

import { EditableSchemaMetadata } from '@types';

type Column = ColumnsType<ExtendedSchemaFields>[number];

/**
 * Whether the field carries the label (tag or glossary term) urn, reading the same sources the
 * Tags / Glossary Terms columns (and the row filter) read — see fieldLabelUrns. `editable` is the
 * field's own editableSchemaMetadata entry (see editableFieldInfoByPath).
 */
export function fieldHasLabel(
    record: ExtendedSchemaFields,
    labelUrn: string,
    editable?: EditableFieldInfo | null,
): boolean {
    const { tags, terms } = fieldLabelUrns(record, editable);
    return tags.includes(labelUrn) || terms.includes(labelUrn);
}

/**
 * One read-only antd column per LABEL kind, keyed `LABEL:<urn>`: a check mark when the field has
 * the tag/term, empty otherwise. Sortable, present rows first.
 */
export function useLabelColumns(
    labelColumns: ColumnLike[],
    editableSchemaMetadata?: EditableSchemaMetadata | null,
): Record<string, Column> {
    const { t } = useTranslation('entity.views');
    return useMemo(() => {
        const result: Record<string, Column> = {};
        // Built once per metadata change; `has` runs per row in sorters and renderers.
        const editableByPath = editableFieldInfoByPath(editableSchemaMetadata);
        labelColumns.forEach((col) => {
            const urn = labelUrnOf(col);
            if (!urn) return;
            const id = columnIdentity(col);
            const has = (record: ExtendedSchemaFields) =>
                fieldHasLabel(record, urn, editableByPath.get(record.fieldPath));
            result[id] = {
                key: id,
                width: 120,
                align: 'center',
                title: columnHeader(col, t),
                sorter: (a, b) => Number(has(b)) - Number(has(a)),
                // TODO(colview): editable presence toggle is a possible future write path
                render: (_: unknown, record: ExtendedSchemaFields) =>
                    has(record) ? <CheckOutlined aria-label={t('columnViews.labelPresent')} /> : null,
            };
        });
        return result;
    }, [labelColumns, editableSchemaMetadata, t]);
}
