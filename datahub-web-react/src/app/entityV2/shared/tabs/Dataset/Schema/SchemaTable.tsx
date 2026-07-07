import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import useSchemaTitleRenderer from '@app/entityV2/dataset/profile/schema/utils/schemaTitleRenderer';
import useSchemaTypeRenderer from '@app/entityV2/dataset/profile/schema/utils/schemaTypeRenderer';
import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import SchemaFieldDrawer from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldDrawer';
import useKeyboardControls from '@app/entityV2/shared/tabs/Dataset/Schema/useKeyboardControls';
import useBusinessAttributeRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useBusinessAttributeRenderer';
import useDescriptionRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useDescriptionRenderer';
import useExtractFieldDescriptionInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldDescriptionInfo';
import useExtractFieldGlossaryTermsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldGlossaryTermsInfo';
import useExtractFieldTagsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldTagsInfo';
import { useGetStructuredPropColumns } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useGetStructuredPropColumns';
import { useGetTableColumnProperties } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useGetTableColumnProperties';
import useTagsAndTermsRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useTagsAndTermsRenderer';
import useUsageStatsRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useUsageStatsRenderer';
import { useBusinessAttributesFlag } from '@app/useAppConfig';
import { EmptyState, Table } from '@src/alchemy-components';
import { Column, SortingState } from '@src/alchemy-components/components/Table/types';
import { useEntityData } from '@src/app/entity/shared/EntityContext';

import { EditableSchemaMetadata, SchemaField, SchemaMetadata, UsageQueryResult } from '@types';

// Minimal shape of the legacy antd column definitions we reuse and adapt to alchemy columns.
interface LegacyColumn {
    title?: React.ReactNode;
    key?: React.Key;
    dataIndex?: string;
    width?: number | string;
    render?: (value: any, record: ExtendedSchemaFields, index: number) => React.ReactNode;
    sorter?: ((a: ExtendedSchemaFields, b: ExtendedSchemaFields) => number) | boolean;
}

const INDENT_PER_DEPTH = 24;
const CARET_SLOT_WIDTH = 20;

const Container = styled.div`
    width: 100%;
    height: inherit;
    padding: 16px;

    /* Persist the selected-row highlight while its detail drawer is open. */
    .schema-selected-row > td {
        background: ${(props) => props.theme.colors.bgSelectedSubtle} !important;
    }
`;

const NameCell = styled.div`
    display: flex;
    align-items: center;
    min-width: 0;
`;

const CaretSlot = styled.span<{ clickable?: boolean }>`
    flex-shrink: 0;
    width: ${CARET_SLOT_WIDTH}px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    cursor: ${(props) => (props.clickable ? 'pointer' : 'default')};
    color: ${(props) => props.theme.colors.textSecondary};
`;

const NameContent = styled.div`
    min-width: 0;
    overflow: hidden;
`;

type Props = {
    rows: Array<ExtendedSchemaFields>;
    schemaMetadata: SchemaMetadata | undefined | null;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    usageStats?: UsageQueryResult | null;
    expandedRowsFromFilter?: Set<string>;
    filterText?: string;
    inputFields?: SchemaField[];
    expandedDrawerFieldPath: string | null;
    setExpandedDrawerFieldPath: (path: string | null) => void;
    openTimelineDrawer?: boolean;
    setOpenTimelineDrawer?: any;
    refetch?: () => void;
    visibleColumns?: string[];
};

const EMPTY_SET: Set<string> = new Set();
const KEYBOARD_CONTROL_DEBOUNCE_MS = 50;

// Attach a `name` alias (== fieldPath) recursively so the alchemy Table can key expansion off `name`.
function withExpansionKeys(rows: ExtendedSchemaFields[]): ExtendedSchemaFields[] {
    return rows.map((row) => ({
        ...row,
        name: row.fieldPath,
        children: row.children ? withExpansionKeys(row.children) : row.children,
    })) as ExtendedSchemaFields[];
}

export default function SchemaTable({
    rows,
    schemaMetadata,
    editableSchemaMetadata,
    usageStats,
    expandedRowsFromFilter = EMPTY_SET,
    filterText = '',
    inputFields,
    expandedDrawerFieldPath,
    setExpandedDrawerFieldPath,
    openTimelineDrawer = false,
    setOpenTimelineDrawer,
    refetch,
    visibleColumns,
}: Props): JSX.Element {
    const { t } = useTranslation('entity.profile.schema');
    const { t: tc } = useTranslation('common.labels');
    const { urn: entityUrn, entityData } = useEntityData();
    const location = useLocation();

    // Reset expandedDrawerFieldPath when URL pathname changes (ignoring query params) to close drawer on a tab change
    useEffect(() => {
        setExpandedDrawerFieldPath(null);
    }, [location.pathname, setExpandedDrawerFieldPath]);

    const schemaFields = schemaMetadata ? schemaMetadata.fields : inputFields;

    const descriptionRender = useDescriptionRenderer(editableSchemaMetadata, false);
    const usageStatsRenderer = useUsageStatsRenderer(usageStats, expandedDrawerFieldPath);
    const tagRenderer = useTagsAndTermsRenderer(
        editableSchemaMetadata,
        {
            showTags: true,
            showTerms: false,
        },
        filterText,
        false,
        true,
    );
    const termRenderer = useTagsAndTermsRenderer(
        editableSchemaMetadata,
        {
            showTags: false,
            showTerms: true,
        },
        filterText,
        false,
        true,
    );
    const extractFieldGlossaryTermsInfo = useExtractFieldGlossaryTermsInfo(editableSchemaMetadata);
    const extractFieldTagsInfo = useExtractFieldTagsInfo(editableSchemaMetadata);
    const extractFieldDescription = useExtractFieldDescriptionInfo(editableSchemaMetadata);
    const businessAttributeRenderer = useBusinessAttributeRenderer(filterText, false);
    const schemaTitleRenderer = useSchemaTitleRenderer(entityUrn, schemaMetadata, filterText);
    const schemaTypeRenderer = useSchemaTypeRenderer();
    const businessAttributesFlag = useBusinessAttributesFlag();

    const tableColumnStructuredProps = useGetTableColumnProperties(entityData?.platform?.urn);
    const structuredPropColumns = useGetStructuredPropColumns(tableColumnStructuredProps);

    const fieldColumn = useMemo<LegacyColumn>(
        () => ({
            width: 200,
            title: tc('name'),
            dataIndex: 'fieldPath',
            key: 'fieldPath',
            render: schemaTitleRenderer,
            sorter: (sourceA, sourceB) =>
                translateFieldPath(sourceA.fieldPath).localeCompare(translateFieldPath(sourceB.fieldPath)),
        }),
        [schemaTitleRenderer, tc],
    );

    const typeColumn = useMemo<LegacyColumn>(
        () => ({
            width: 100,
            title: tc('type'),
            dataIndex: 'type',
            key: 'type',
            render: schemaTypeRenderer,
            sorter: (sourceA, sourceB) => sourceA.type.localeCompare(sourceB.type),
        }),
        [schemaTypeRenderer, tc],
    );

    const descriptionColumn = useMemo<LegacyColumn>(
        () => ({
            title: tc('description'),
            dataIndex: 'description',
            key: 'description',
            render: descriptionRender,
            sorter: (sourceA, sourceB) =>
                (extractFieldDescription(sourceA).sanitizedDescription ? 1 : 0) -
                (extractFieldDescription(sourceB).sanitizedDescription ? 1 : 0),
        }),
        [descriptionRender, extractFieldDescription, tc],
    );

    const tagColumn = useMemo<LegacyColumn>(
        () => ({
            width: 100,
            title: tc('tags'),
            dataIndex: 'globalTags',
            key: 'tag',
            render: tagRenderer,
            sorter: (sourceA, sourceB) =>
                extractFieldTagsInfo(sourceA).numberOfTags - extractFieldTagsInfo(sourceB).numberOfTags,
        }),
        [tagRenderer, extractFieldTagsInfo, tc],
    );

    const termColumn = useMemo<LegacyColumn>(
        () => ({
            width: 200,
            title: t('schemaTable.glossaryTermsColumn'),
            dataIndex: 'globalTags',
            key: 'term',
            render: termRenderer,
            sorter: (sourceA, sourceB) =>
                extractFieldGlossaryTermsInfo(sourceA).numberOfTerms -
                extractFieldGlossaryTermsInfo(sourceB).numberOfTerms,
        }),
        [termRenderer, extractFieldGlossaryTermsInfo, t],
    );

    const businessAttributeColumn = useMemo<LegacyColumn>(
        () => ({
            width: 150,
            title: t('schemaTable.businessAttributeColumn'),
            dataIndex: 'businessAttribute',
            key: 'businessAttribute',
            render: businessAttributeRenderer,
        }),
        [businessAttributeRenderer, t],
    );

    // Function to get the count of each usageStats fieldPath
    const getCount = useCallback(
        (fieldPath: any) => {
            const data: any =
                usageStats?.aggregations?.fields &&
                usageStats?.aggregations?.fields?.find((field) => {
                    return field?.fieldName === fieldPath;
                });
            return (data && data.count) ?? 0;
        },
        [usageStats],
    );

    const usageColumn = useMemo<LegacyColumn>(
        () => ({
            width: 100,
            title: t('schemaTable.statsColumn'),
            dataIndex: 'fieldPath',
            key: 'usage',
            render: usageStatsRenderer,
            sorter: (sourceA, sourceB) => getCount(sourceA.fieldPath) - getCount(sourceB.fieldPath),
        }),
        [usageStatsRenderer, getCount, t],
    );

    const allColumns = useMemo<LegacyColumn[]>(() => {
        let columns: LegacyColumn[] = [fieldColumn, typeColumn, descriptionColumn, tagColumn, termColumn, usageColumn];

        if (businessAttributesFlag) {
            columns = [...columns, businessAttributeColumn];
        }

        if (structuredPropColumns) columns.splice(columns?.length - 1, 0, ...(structuredPropColumns as LegacyColumn[]));
        return columns;
    }, [
        fieldColumn,
        typeColumn,
        businessAttributeColumn,
        descriptionColumn,
        tagColumn,
        termColumn,
        usageColumn,
        structuredPropColumns,
        businessAttributesFlag,
    ]);

    const legacyColumns = useMemo<LegacyColumn[]>(() => {
        if (!visibleColumns) return allColumns;
        return allColumns.filter((column) => column.key && visibleColumns?.includes(column.key.toString()));
    }, [allColumns, visibleColumns]);

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
    const [sortColumn, setSortColumn] = useState<string | null>(null);
    const [sortOrder, setSortOrder] = useState<SortingState>(SortingState.ORIGINAL);

    useEffect(() => {
        setExpandedRows((previousRows) => {
            const finalRowsSet = new Set<string>();
            expandedRowsFromFilter.forEach((row) => finalRowsSet.add(row));
            previousRows.forEach((row) => finalRowsSet.add(row));
            return finalRowsSet;
        });
    }, [expandedRowsFromFilter]);

    const toggleExpanded = useCallback((fieldPath: string) => {
        setExpandedRows((previousRows) => {
            const next = new Set(previousRows);
            if (next.has(fieldPath)) {
                next.delete(fieldPath);
            } else {
                next.add(fieldPath);
            }
            return next;
        });
    }, []);

    // Active sorter mirrors the alchemy Table's internal sort state so we can apply the same
    // ordering to nested children (which render in their own inner tables).
    const activeSorter = useMemo(() => {
        const col = legacyColumns.find((c) => c.key?.toString() === sortColumn);
        return typeof col?.sorter === 'function' ? col.sorter : undefined;
    }, [legacyColumns, sortColumn]);

    const sortLevel = useCallback(
        (levelRows: ExtendedSchemaFields[]): ExtendedSchemaFields[] => {
            if (sortOrder === SortingState.ORIGINAL || !sortColumn || !activeSorter) return levelRows;
            return levelRows
                .slice()
                .sort((a, b) => (sortOrder === SortingState.ASCENDING ? activeSorter(a, b) : activeSorter(b, a)));
        },
        [sortOrder, sortColumn, activeSorter],
    );

    const columns = useMemo<Column<ExtendedSchemaFields>[]>(() => {
        return legacyColumns.map((col) => {
            const key = String(col.key);
            const base = {
                title: col.title ?? '',
                key,
                width: typeof col.width === 'number' ? `${col.width}px` : col.width,
                sorter: typeof col.sorter === 'function' ? col.sorter : undefined,
            };

            if (key === 'fieldPath') {
                return {
                    ...base,
                    render: (record: ExtendedSchemaFields) => {
                        const canExpand = !!record.children?.length;
                        const isExpanded = expandedRows.has(record.fieldPath);
                        return (
                            <NameCell style={{ paddingLeft: (record.depth || 0) * INDENT_PER_DEPTH }}>
                                <CaretSlot
                                    clickable={canExpand}
                                    onClick={(e) => {
                                        if (!canExpand) return;
                                        e.stopPropagation();
                                        toggleExpanded(record.fieldPath);
                                    }}
                                >
                                    {canExpand &&
                                        (isExpanded ? (
                                            <CaretDown size={14} weight="bold" />
                                        ) : (
                                            <CaretRight size={14} weight="bold" />
                                        ))}
                                </CaretSlot>
                                <NameContent>{schemaTitleRenderer(record.fieldPath, record)}</NameContent>
                            </NameCell>
                        );
                    },
                };
            }

            return {
                ...base,
                render: (record: ExtendedSchemaFields, index: number) => {
                    const value = col.dataIndex ? (record as any)[col.dataIndex] : undefined;
                    return col.render ? col.render(value, record, index) : value;
                },
            };
        });
    }, [legacyColumns, expandedRows, toggleExpanded, schemaTitleRenderer]);

    const namedRows = useMemo(() => withExpansionKeys(rows), [rows]);

    // Flattened list of currently-visible rows (respecting expansion + active sort) for keyboard
    // navigation and the field drawer. Rendering itself uses true nested tables (below).
    const displayedRows = useMemo(() => {
        const out: ExtendedSchemaFields[] = [];
        const walk = (levelRows: ExtendedSchemaFields[]) => {
            sortLevel(levelRows).forEach((record) => {
                out.push(record);
                if (expandedRows.has(record.fieldPath) && record.children) {
                    walk(record.children);
                }
            });
        };
        walk(namedRows);
        return out;
    }, [namedRows, expandedRows, sortLevel]);

    const rowClassName = useCallback(
        (record: ExtendedSchemaFields) => (expandedDrawerFieldPath === record.fieldPath ? 'schema-selected-row' : ''),
        [expandedDrawerFieldPath],
    );

    const rowDataTestId = useCallback((record: ExtendedSchemaFields) => `schema-field-${record.fieldPath}`, []);

    const onRowClick = useCallback(
        (record: ExtendedSchemaFields) => {
            setExpandedDrawerFieldPath(expandedDrawerFieldPath === record.fieldPath ? null : record.fieldPath);
        },
        [expandedDrawerFieldPath, setExpandedDrawerFieldPath],
    );

    const containerRef = useRef<HTMLDivElement>(null);

    // Scroll the selected field into view (covers keyboard nav + deep-linked field on load).
    useDebounce(
        () => {
            if (!expandedDrawerFieldPath || !containerRef.current) return;
            const row = containerRef.current.querySelector(
                `[data-testid="schema-field-${CSS.escape(expandedDrawerFieldPath)}"]`,
            );
            row?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
        },
        KEYBOARD_CONTROL_DEBOUNCE_MS,
        [expandedDrawerFieldPath, filterText, sortColumn, sortOrder],
    );

    const { selectPreviousField, selectNextField } = useKeyboardControls(
        displayedRows,
        expandedDrawerFieldPath,
        setExpandedDrawerFieldPath,
        expandedRows,
        setExpandedRows,
    );

    const [schemaFieldDrawerFieldPath, setSchemaFieldDrawerFieldPath] = useState(expandedDrawerFieldPath);
    useDebounce(() => setSchemaFieldDrawerFieldPath(expandedDrawerFieldPath), KEYBOARD_CONTROL_DEBOUNCE_MS, [
        expandedDrawerFieldPath,
    ]);

    // Recursive expandable config: an expanded row renders its children in a nested (borderless)
    // alchemy Table that reuses this same config, so nesting works to any depth. Children are
    // pre-sorted with the active sorter to stay consistent with the top-level ordering.
    const expandable = useMemo(() => {
        const config: any = {
            expandedGroupIds: Array.from(expandedRows),
            expandedRowRender: (record: ExtendedSchemaFields) => (
                <Table
                    columns={columns}
                    data={sortLevel(record.children || [])}
                    showHeader={false}
                    isBorderless
                    isExpandedInnerTable
                    expandable={config}
                    onRowClick={onRowClick}
                    rowClassName={rowClassName}
                    rowDataTestId={rowDataTestId}
                />
            ),
        };
        return config;
    }, [columns, expandedRows, sortLevel, onRowClick, rowClassName, rowDataTestId]);

    return (
        <>
            <Container ref={containerRef} data-testid="schema-table-container">
                {displayedRows.length === 0 ? (
                    <EmptyState title={t('schemaTable.empty', { defaultValue: 'No columns' })} />
                ) : (
                    <Table
                        data-testid="schema-table"
                        columns={columns}
                        data={namedRows}
                        expandable={expandable}
                        onRowClick={onRowClick}
                        rowClassName={rowClassName}
                        rowDataTestId={rowDataTestId}
                        isScrollable
                        maxHeight="100%"
                        handleSortColumnChange={({ sortColumn: nextColumn, sortOrder: nextOrder }) => {
                            setSortColumn(nextColumn);
                            setSortOrder(nextOrder);
                        }}
                    />
                )}
            </Container>
            {!!schemaFields && (
                <SchemaFieldDrawer
                    schemaFields={schemaFields}
                    expandedDrawerFieldPath={schemaFieldDrawerFieldPath}
                    editableSchemaMetadata={editableSchemaMetadata}
                    setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                    openTimelineDrawer={openTimelineDrawer}
                    setOpenTimelineDrawer={setOpenTimelineDrawer}
                    selectPreviousField={selectPreviousField}
                    selectNextField={selectNextField}
                    usageStats={usageStats}
                    displayedRows={displayedRows}
                    refetch={refetch}
                />
            )}
        </>
    );
}
