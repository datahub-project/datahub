import { CheckOutlined } from '@ant-design/icons';
import type { ColumnType } from 'antd/es/table';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { deriveTypeParts, formatPrecisionScale } from '@app/entityV2/columnView/deriveTypeParts';

/** The schema-row attributes these columns read; structurally satisfied by ExtendedSchemaFields. */
export interface FieldAttributeRow {
    nativeDataType?: string | null;
    type?: string | null;
    nullable?: boolean | null;
    isPartOfKey?: boolean | null;
    isPartitioningKey?: boolean | null;
}

/** Keyed by DataHubColumnViewColumnType name, ready to spread into SchemaTable's `byKind`. */
export interface FieldAttributeColumns<R extends FieldAttributeRow> {
    NATIVE_TYPE: ColumnType<R>;
    LENGTH: ColumnType<R>;
    PRECISION_SCALE: ColumnType<R>;
    NULLABLE: ColumnType<R>;
    PRIMARY_KEY: ColumnType<R>;
    PARTITION_KEY: ColumnType<R>;
}

const num = (v: number | undefined) => (v === undefined ? Number.NEGATIVE_INFINITY : v);
const flag = (v: boolean | null | undefined) => (v ? 1 : 0);

/** Read-only check mark — the same idiom as the Label presence columns. */
function CheckCell({ on, label }: { on: boolean | null | undefined; label: string }) {
    return on ? (
        <span role="img" aria-label={label} title={label}>
            <CheckOutlined />
        </span>
    ) : (
        <span aria-hidden>—</span>
    );
}

/**
 * Schema-table columns for field attributes the legacy table only showed as a tooltip (native type)
 * or as pills on the name (nullable / primary key / partition key), plus length and precision/scale
 * derived from the native type. All are plain row attributes: sortable, no extra fetch.
 */
export function useFieldAttributeColumns<R extends FieldAttributeRow>(): FieldAttributeColumns<R> {
    const { t } = useTranslation('entity.views');
    const { t: ts } = useTranslation('entity.profile.schema');

    return useMemo(() => {
        const parts = (row: R) => deriveTypeParts(row.nativeDataType, row.type);
        const check = (key: string, label: string, get: (row: R) => boolean | null | undefined): ColumnType<R> => ({
            key,
            title: label,
            align: 'center',
            render: (_: unknown, row: R) => <CheckCell on={get(row)} label={label} />,
            sorter: (a: R, b: R) => flag(get(a)) - flag(get(b)),
        });
        return {
            NATIVE_TYPE: {
                key: 'nativeType',
                title: t('columnViews.kind.nativeType'),
                ellipsis: true,
                render: (_: unknown, row: R) => <span title={row.nativeDataType || ''}>{row.nativeDataType || '—'}</span>,
                sorter: (a: R, b: R) => (a.nativeDataType || '').localeCompare(b.nativeDataType || ''),
            },
            LENGTH: {
                key: 'length',
                title: t('columnViews.kind.length'),
                align: 'right',
                render: (_: unknown, row: R) => {
                    const { length } = parts(row);
                    return <span>{length === undefined ? '' : length.toLocaleString()}</span>;
                },
                sorter: (a: R, b: R) => num(parts(a).length) - num(parts(b).length),
            },
            PRECISION_SCALE: {
                key: 'precisionScale',
                title: t('columnViews.kind.precisionScale'),
                align: 'right',
                render: (_: unknown, row: R) => <span>{formatPrecisionScale(parts(row))}</span>,
                sorter: (a: R, b: R) => {
                    const pa = parts(a);
                    const pb = parts(b);
                    return num(pa.precision) - num(pb.precision) || num(pa.scale) - num(pb.scale);
                },
            },
            NULLABLE: check('nullable', ts('constraintLabels.nullable'), (row) => row.nullable),
            PRIMARY_KEY: check('primaryKey', ts('constraintLabels.primaryKey'), (row) => row.isPartOfKey),
            PARTITION_KEY: check('partitionKey', ts('constraintLabels.partitionKey'), (row) => row.isPartitioningKey),
        };
    }, [t, ts]);
}
