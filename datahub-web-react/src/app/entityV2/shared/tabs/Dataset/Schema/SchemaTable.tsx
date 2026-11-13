import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import { Table } from '@components/components/Table';
import { Column, SortingState, TableSortState, TableTreeDataProps } from '@components/components/Table/types';
import { getSortedData } from '@components/components/Table/utils';

import useSchemaTitleRenderer from '@app/entityV2/dataset/profile/schema/utils/schemaTitleRenderer';
import useSchemaTypeRenderer from '@app/entityV2/dataset/profile/schema/utils/schemaTypeRenderer';
import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import { hasNestedSchemaRows } from '@app/entityV2/dataset/profile/schema/utils/utils';
import SchemaFieldDepthBadge from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDepthBadge';
import SchemaFieldDrawer from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldDrawer';
import {
    MetadataStatus,
    renderMetadataCell as renderMetadataCellFor,
} from '@app/entityV2/shared/tabs/Dataset/Schema/metadataStatus';
import useKeyboardControls from '@app/entityV2/shared/tabs/Dataset/Schema/useKeyboardControls';
import { getFieldUsageCount } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/statsUtil';
import useBusinessAttributeRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useBusinessAttributeRenderer';
import useDescriptionRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useDescriptionRenderer';
import useEditableSchemaFieldInfoMaps from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useEditableSchemaFieldInfoMaps';
import useExtractFieldDescriptionInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldDescriptionInfo';
import useExtractFieldGlossaryTermsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldGlossaryTermsInfo';
import useExtractFieldTagsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldTagsInfo';
import { useGetStructuredPropColumns } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useGetStructuredPropColumns';
import { useGetTableColumnProperties } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useGetTableColumnProperties';
import useTagsAndTermsRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useTagsAndTermsRenderer';
import useUsageStatsRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useUsageStatsRenderer';
import { useBusinessAttributesFlag } from '@app/useAppConfig';
import { useEntityData } from '@src/app/entityV2/shared/EntityContext';

import { EditableSchemaMetadata, SchemaField, SchemaMetadata, UsageQueryResult } from '@types';

const TableContainer = styled.div`
    height: inherit;
    overflow: hidden;
`;

const NameCell = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
`;

type Props = {
    rows: Array<ExtendedSchemaFields>;
    schemaMetadata: SchemaMetadata | undefined | null;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    editedPrimaryKeys?: string[] | null;
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
    /** Phase 2 (full metadata) state; drives skeleton / unavailable rendering of metadata cells. */
    metadataStatus?: MetadataStatus;
};

const EMPTY_SET: Set<string> = new Set();
const NAME_COLUMN_WIDTH = '240px';
const UNSORTED: TableSortState = { sortColumn: null, sortOrder: SortingState.ORIGINAL };
const KEYBOARD_CONTROL_DEBOUNCE_MS = 50;
// Virtualized rows are exactly this tall, matching the previous antd schema table.
const SCHEMA_ROW_HEIGHT = 45;
// A constant so the table's virtualized window keeps one scroll listener across renders. Schema cells
// are heavy (descriptions, tags, properties), so only a few rows render beyond the viewport.
const VIRTUALIZATION = { rowHeight: SCHEMA_ROW_HEIGHT, overscan: 5 };
// Depth 0 and 1 are obvious from the indent; deeper rows (and every row while search flattens
// the tree's context) get the depth badge.
const MIN_DEPTH_FOR_BADGE = 2;

export default function SchemaTable({
    rows,
    schemaMetadata,
    editableSchemaMetadata,
    editedPrimaryKeys,
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
    metadataStatus = 'ready',
}: Props): JSX.Element {
    const { t } = useTranslation('entity.profile.schema');
    const { t: tc } = useTranslation('common.labels');
    const { urn: entityUrn, entityData } = useEntityData();
    const location = useLocation();

    // Reset expandedDrawerFieldPath when URL pathname changes (ignoring query params) to close drawer on a tab change
    useEffect(() => {
        setExpandedDrawerFieldPath(null);
    }, [location.pathname, setExpandedDrawerFieldPath]);

    const [sortState, setSortState] = useState<TableSortState>(UNSORTED);
    const isSearchActive = filterText !== '';

    const schemaFields = schemaMetadata ? schemaMetadata.fields : inputFields;

    const fieldInfoMaps = useEditableSchemaFieldInfoMaps(editableSchemaMetadata);
    const descriptionRender = useDescriptionRenderer(editableSchemaMetadata, false, fieldInfoMaps);
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
        fieldInfoMaps,
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
        fieldInfoMaps,
    );
    const extractFieldGlossaryTermsInfo = useExtractFieldGlossaryTermsInfo(editableSchemaMetadata, fieldInfoMaps);
    const extractFieldTagsInfo = useExtractFieldTagsInfo(editableSchemaMetadata, fieldInfoMaps);
    const extractFieldDescription = useExtractFieldDescriptionInfo(editableSchemaMetadata, fieldInfoMaps);
    const businessAttributeRenderer = useBusinessAttributeRenderer(filterText, false);
    const schemaTitleRenderer = useSchemaTitleRenderer(entityUrn, schemaMetadata, editedPrimaryKeys, filterText);
    const schemaTypeRenderer = useSchemaTypeRenderer();
    const businessAttributesFlag = useBusinessAttributesFlag();

    const tableColumnStructuredProps = useGetTableColumnProperties(entityData?.platform?.urn);
    const structuredPropColumns = useGetStructuredPropColumns(tableColumnStructuredProps, metadataStatus);

    const hasNestedRows = useMemo(() => hasNestedSchemaRows(rows), [rows]);

    const fieldColumn = useMemo(
        (): Column<ExtendedSchemaFields> => ({
            fixed: 'left',
            width: NAME_COLUMN_WIDTH,
            // Only the pinned Name column keeps a floor; the rest shrink so the last columns stay in view.
            minWidth: NAME_COLUMN_WIDTH,
            title: tc('name'),
            key: 'fieldPath',
            render: (record) => {
                const depth = record.depth ?? 0;
                const showDepthBadge = depth >= MIN_DEPTH_FOR_BADGE || (isSearchActive && hasNestedRows);
                return (
                    <NameCell>
                        {showDepthBadge && <SchemaFieldDepthBadge depth={depth} />}
                        {schemaTitleRenderer(record.fieldPath, record)}
                    </NameCell>
                );
            },
            sorter: (sourceA, sourceB) =>
                translateFieldPath(sourceA.fieldPath).localeCompare(translateFieldPath(sourceB.fieldPath)),
        }),
        [schemaTitleRenderer, tc, isSearchActive, hasNestedRows],
    );

    const typeColumn = useMemo(
        (): Column<ExtendedSchemaFields> => ({
            width: '100px',
            title: tc('type'),
            key: 'type',
            render: schemaTypeRenderer,
            sorter: (sourceA, sourceB) => sourceA.type.localeCompare(sourceB.type),
        }),
        [schemaTypeRenderer, tc],
    );

    // One shared wrapper for every Phase 2 metadata column (description/tags/terms): skeleton
    // while full metadata loads, an explicit "unavailable" marker when the full query failed
    // (a blank cell would read as "no tags"), real content otherwise.
    const renderMetadataCell = useCallback(
        (width: number, content: () => React.ReactNode): React.ReactNode =>
            renderMetadataCellFor(metadataStatus, width, content),
        [metadataStatus],
    );

    const descriptionColumn = useMemo(
        (): Column<ExtendedSchemaFields> => ({
            width: '400px',
            title: tc('description'),
            key: 'description',
            render: (record) =>
                renderMetadataCell(120, () => descriptionRender(record.description ?? undefined, record)),
            sorter: (sourceA, sourceB) =>
                (extractFieldDescription(sourceA).sanitizedDescription ? 1 : 0) -
                (extractFieldDescription(sourceB).sanitizedDescription ? 1 : 0),
        }),
        [descriptionRender, extractFieldDescription, tc, renderMetadataCell],
    );

    const tagColumn = useMemo(
        (): Column<ExtendedSchemaFields> => ({
            width: '100px',
            title: tc('tags'),
            key: 'tag',
            render: (record) => renderMetadataCell(80, () => tagRenderer(record.globalTags, record)),
            sorter: (sourceA, sourceB) =>
                extractFieldTagsInfo(sourceA).numberOfTags - extractFieldTagsInfo(sourceB).numberOfTags,
        }),
        [tagRenderer, extractFieldTagsInfo, tc, renderMetadataCell],
    );

    const termColumn = useMemo(
        (): Column<ExtendedSchemaFields> => ({
            width: '200px',
            title: t('schemaTable.glossaryTermsColumn'),
            key: 'term',
            render: (record) => renderMetadataCell(80, () => termRenderer(record.globalTags, record)),
            sorter: (sourceA, sourceB) =>
                extractFieldGlossaryTermsInfo(sourceA).numberOfTerms -
                extractFieldGlossaryTermsInfo(sourceB).numberOfTerms,
        }),
        [termRenderer, extractFieldGlossaryTermsInfo, t, renderMetadataCell],
    );

    const businessAttributeColumn = useMemo(
        (): Column<ExtendedSchemaFields> => ({
            width: '150px',
            title: t('schemaTable.businessAttributeColumn'),
            key: 'businessAttribute',
            render: businessAttributeRenderer,
        }),
        [businessAttributeRenderer, t],
    );

    const usageColumn = useMemo(
        (): Column<ExtendedSchemaFields> => ({
            width: '100px',
            title: t('schemaTable.statsColumn'),
            key: 'usage',
            render: (record) => usageStatsRenderer(record.fieldPath),
            sorter: (sourceA, sourceB) =>
                getFieldUsageCount(usageStats, sourceA.fieldPath) - getFieldUsageCount(usageStats, sourceB.fieldPath),
        }),
        [usageStatsRenderer, usageStats, t],
    );

    const tableColumns = useMemo(() => {
        let columns: Column<ExtendedSchemaFields>[] = [
            fieldColumn,
            typeColumn,
            descriptionColumn,
            tagColumn,
            termColumn,
            usageColumn,
        ];

        if (businessAttributesFlag) {
            columns = [...columns, businessAttributeColumn];
        }

        if (structuredPropColumns) columns.splice(columns.length - 1, 0, ...structuredPropColumns);
        if (visibleColumns) columns = columns.filter((column) => visibleColumns.includes(column.key));
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
        visibleColumns,
    ]);

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

    useEffect(() => {
        setExpandedRows((previousRows) => new Set([...expandedRowsFromFilter, ...previousRows]));
    }, [expandedRowsFromFilter]);

    // Memoized: whenever this object changes, the table re-sorts and re-flattens the whole field tree.
    const treeData = useMemo(
        (): TableTreeDataProps<ExtendedSchemaFields> => ({
            getChildren: (field) => field.children,
            expandedRowKeys: Array.from(expandedRows),
            onExpandedRowsChange: (keys) => setExpandedRows(new Set(keys)),
        }),
        [expandedRows],
    );

    // The rows on screen in table order (children sorted within their parent, like the table),
    // so keyboard navigation and the drawer's previous/next follow what the user sees.
    const displayedRows = useMemo(() => {
        const visibleRows: ExtendedSchemaFields[] = [];
        const collect = (fields: ExtendedSchemaFields[]) => {
            getSortedData(tableColumns, fields, sortState.sortColumn, sortState.sortOrder).forEach((field) => {
                visibleRows.push(field);
                if (field.children && expandedRows.has(field.fieldPath)) collect(field.children);
            });
        };
        collect(rows);
        return visibleRows;
    }, [rows, expandedRows, tableColumns, sortState]);

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

    return (
        <>
            <TableContainer data-testid="schema-table-container">
                <Table<ExtendedSchemaFields>
                    data-testid="schema-table"
                    columns={tableColumns}
                    data={rows}
                    rowKey="fieldPath"
                    isScrollable
                    maxHeight="100%"
                    virtualization={VIRTUALIZATION}
                    treeData={treeData}
                    sortState={sortState}
                    onSortChange={setSortState}
                    focusedRowKey={expandedDrawerFieldPath}
                    scrollToRowKey={expandedDrawerFieldPath}
                    onRowClick={(record) =>
                        setExpandedDrawerFieldPath(
                            expandedDrawerFieldPath === record.fieldPath ? null : record.fieldPath,
                        )
                    }
                    rowDataTestId={(record) => `schema-field-${record.fieldPath}`}
                />
            </TableContainer>
            {!!schemaFields && (
                <SchemaFieldDrawer
                    schemaFields={schemaFields}
                    schemaPrimaryKeys={schemaMetadata?.primaryKeys}
                    editedPrimaryKeys={editedPrimaryKeys}
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
