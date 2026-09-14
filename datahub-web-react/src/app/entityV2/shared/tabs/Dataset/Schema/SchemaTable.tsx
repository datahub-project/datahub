import { Typography } from 'antd';
import { ColumnsType } from 'antd/es/table';
import { SorterResult } from 'antd/lib/table/interface';
import ResizeObserver from 'rc-resize-observer';
import type { FixedType } from 'rc-table/lib/interface';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled from 'styled-components';
import { useVT } from 'virtualizedtableforantd4';

import { useColumnViewContext } from '@app/entityV2/columnView/ColumnViewContext';
import ResizableHeaderCell from '@app/entityV2/columnView/ResizableHeaderCell';
import {
    ColumnLike,
    columnIdentity,
    isLabelColumn,
    isRelationshipColumn,
    isStructuredPropertyColumn,
    structuredPropertyUrnOf,
    withDisplay,
} from '@app/entityV2/columnView/columnKinds';
import { filterSchemaRowsByView } from '@app/entityV2/columnView/filterSchemaRowsByView';
import {
    defaultSortForView,
    legacyDefaultColumns,
    resolveSchemaTableColumns,
} from '@app/entityV2/columnView/resolveSchemaTableColumns';
import { useRelationshipColumns } from '@app/entityV2/columnView/useRelationshipColumns';
import { useVisibleRowUrns } from '@app/entityV2/columnView/useVisibleRowUrns';
import { useFieldAttributeColumns } from '@app/entityV2/columnView/useFieldAttributeColumns';
import { useLabelColumns } from '@app/entityV2/columnView/useLabelColumns';
import SchemaRow from '@app/entityV2/dataset/profile/schema/components/SchemaRow';
import useSchemaTitleRenderer from '@app/entityV2/dataset/profile/schema/utils/schemaTitleRenderer';
import useSchemaTypeRenderer from '@app/entityV2/dataset/profile/schema/utils/schemaTypeRenderer';
import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import { findIndexOfFieldPathExcludingCollapsedFields } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { StyledTable } from '@app/entityV2/shared/components/styled/StyledTable';
import ExpandIcon from '@app/entityV2/shared/tabs/Dataset/Schema/components/ExpandIcon';
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
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useEntityData } from '@src/app/entity/shared/EntityContext';

import { EditableSchemaMetadata, SchemaField, SchemaMetadata, UsageQueryResult } from '@types';

const ViewFilterSummary = styled.div`
    padding: 4px 16px;
    font-size: 12px;
    color: ${(p) => p.theme.colors.textSecondary};
`;

const TableContainer = styled.div<{ isSearchActive: boolean; hasRowWithDepth: boolean }>`
    overflow: inherit;
    height: inherit;

    &&& .ant-table-tbody > tr > .ant-table-cell-with-append {
        border-right: none;
        padding: 0px;
    }

    &&& .ant-table-tbody > tr {
        background-color: ${(props) => props.theme.colors.bg};
    }

    &&& .ant-table-tbody > tr.expanded-child {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }

    &&& .ant-table-tbody > tr > .ant-table-cell {
        border-right: none;
    }

    &&& .open-fk-row > td {
        padding-bottom: 600px;
        vertical-align: top;
    }

    &&& .ant-table-cell {
        max-height: 45px !important;
        height: 45px !important;
        background-color: inherit;
        cursor: pointer;
        padding-top: 0px;
        padding-bottom: 0px;
    }

    &&& .selected-row * {
        .ant-typography mark {
            background-color: ${(props) => props.theme.colors.bgHighlight} !important;
        }

        .row-icon-tooltip .ant-tooltip-inner {
            background: ${(props) => props.theme.colors.bgSurface} !important;
            color: ${(props) => props.theme.colors.text} !important;
        }

        .ant-tag {
            background-color: ${(props) => props.theme.colors.bg};
        }
    }

    &&& .selected-row {
        background: ${(props) => props.theme.colors.border} !important;
    }

    &&& .level-0 td .row-icon-container .row-icon {
        ${(props) => (props.isSearchActive && props.hasRowWithDepth ? '' : `display: none;`)}
    }

    &&& .level-1 td .row-icon-container .row-icon {
        ${(props) => (props.isSearchActive && props.hasRowWithDepth ? '' : `display: none;`)}
    }

    &&& tr.expanded-row td:first-of-type {
        border-left: ${(props) =>
            props.isSearchActive ? '4px solid transparent' : `4px solid ${props.theme.colors.bgSurfaceBrand}`};
    }

    &&& .expanded-child > td {
        .depth-container {
            background: ${(props) => props.theme.colors.bgSurfaceBrand};
        }

        .depth-text {
            background: transparent;
        }
    }

    &&& .description-column {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 400px;
    }

    // this makes the table fill up height of parent

    .ant-spin-nested-loading {
        height: 100%;

        .ant-spin-container {
            height: 100%;

            .ant-table {
                height: 100%;

                .ant-table-container {
                    height: 100%;

                    .ant-table-body {
                        height: 100%;
                    }

                    .ant-table-body > div:first-child {
                        height: 100%;
                    }
                }
            }
        }
    }
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
    matches?: {
        path: string;
        index: number;
    }[];
    refetch?: () => void;
    visibleColumns?: string[];
};

const EMPTY_SET: Set<string> = new Set();
const TABLE_HEADER_HEIGHT = 52;
const KEYBOARD_CONTROL_DEBOUNCE_MS = 50;
const SCROLL_X = 'max-content';

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

    const [tableHeight, setTableHeight] = useState(0);
    const [schemaSorter, setSchemaSorter] = useState<SorterResult<any> | undefined>(undefined);

    const [isSearchActive, setIsSearchActive] = useState<boolean>(false);

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

    // Column Views: with an active view, structured-property columns come from the view's own
    // (caller-context-resolved) properties; otherwise from the platform-flagged legacy source.
    const { activeDefinition, setAdHocDefinition, selectedColumnView, isAdHocModified } = useColumnViewContext();
    const tableColumnStructuredProps = useGetTableColumnProperties(entityData?.platform?.urn);
    const viewStructuredProps = useMemo(
        () =>
            activeDefinition?.columns.filter(isStructuredPropertyColumn).flatMap((c) => {
                const resolved = c.structuredPropertyParams?.structuredProperty;
                if (resolved) return [{ entity: resolved } as any];
                // Synthesized from the built-in layout (legacyDefaultColumns) — only the urn is known,
                // so the entity comes from the platform-flagged source it was synthesized from.
                const urn = structuredPropertyUrnOf(c);
                const flagged = tableColumnStructuredProps?.find((r) => r.entity.urn === urn);
                return flagged ? [flagged] : [];
            }),
        [activeDefinition, tableColumnStructuredProps],
    );
    const structuredPropColumns = useGetStructuredPropColumns(
        activeDefinition ? viewStructuredProps : tableColumnStructuredProps,
    );
    // Row filter from the active definition: client-side over already-fetched rows (pure projection).
    // editableSchemaMetadata is passed so tags / glossaryTerms clauses see user-added labels too.
    const viewFilter = useMemo(
        () => filterSchemaRowsByView(rows, activeDefinition?.filter, editableSchemaMetadata),
        [rows, activeDefinition, editableSchemaMetadata],
    );
    const isViewFilterActive = viewFilter.shown !== viewFilter.total;
    const clearViewFilter = () =>
        activeDefinition && setAdHocDefinition({ ...activeDefinition, filter: null } as any);
    // Relationship (GRAPH) columns: fetched for VISIBLE rows only, outside the main schema query.
    // A saved, unmodified view is passed so the server applies that column's own display.maxItems.
    const visibleRows = useVisibleRowUrns();
    const relationshipColumns = useRelationshipColumns(
        activeDefinition?.columns.filter(isRelationshipColumn) || [],
        visibleRows,
        { columnViewUrn: !isAdHocModified ? selectedColumnView?.urn : undefined },
    );
    // LABEL columns: read-only presence of one tag/term, from the same sources as the Tags/Terms columns.
    const labelColumns = useLabelColumns(activeDefinition?.columns.filter(isLabelColumn) || [], editableSchemaMetadata);
    const { t: tv } = useTranslation('entity.views');
    const entityRegistry = useEntityRegistry();

    const fieldColumn = useMemo(
        () => ({
            fixed: 'left' as FixedType,
            width: 200,
            title: tc('name'),
            dataIndex: 'fieldPath',
            key: 'fieldPath',
            render: schemaTitleRenderer,
            filtered: true,
            onCell: () => ({ style: { whiteSpace: 'pre' as const } }),
            sorter: (sourceA, sourceB) =>
                translateFieldPath(sourceA.fieldPath).localeCompare(translateFieldPath(sourceB.fieldPath)),
        }),
        [schemaTitleRenderer, tc],
    );

    // Column Views: attribute columns the legacy table never had (native type, length,
    // precision/scale, nullable, primary key, partition key); shown only when a view selects them.
    const fieldAttributeColumns = useFieldAttributeColumns<ExtendedSchemaFields>();

    const typeColumn = useMemo(
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

    const descriptionColumn = useMemo(
        () => ({
            ellipsis: true,
            className: 'description-column',
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

    const tagColumn = useMemo(
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

    const termColumn = useMemo(
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

    const businessAttributeColumn = useMemo(
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

    const usageColumn = useMemo(
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

    const columnSources = useMemo(
        () => ({
            fieldColumn,
            byKind: {
                TYPE: typeColumn,
                DESCRIPTION: descriptionColumn,
                TAGS: tagColumn,
                GLOSSARY_TERMS: termColumn,
                STATS: usageColumn,
                BUSINESS_ATTRIBUTE: businessAttributesFlag ? businessAttributeColumn : undefined,
                // Native type, length, precision/scale, nullable, primary key, partition key.
                ...fieldAttributeColumns,
            },
            structuredPropColumns,
            labelColumns,
            relationshipColumns,
            // Column Views: lets a view render Tags / Glossary Terms as CHECK / COUNT (renderer registry).
            cellCounters: {
                TAGS: (record) => extractFieldTagsInfo(record).numberOfTags,
                GLOSSARY_TERMS: (record) => extractFieldGlossaryTermsInfo(record).numberOfTerms,
            },
            rendererContext: {
                t: (key, opts) => tv(key, opts) as string,
                entityUrl: (type, urn) => entityRegistry.getEntityUrl(type, urn),
                limit: 0,
            },
        }),
        [
            fieldColumn,
            typeColumn,
            fieldAttributeColumns,
            businessAttributeColumn,
            descriptionColumn,
            tagColumn,
            termColumn,
            usageColumn,
            structuredPropColumns,
            labelColumns,
            relationshipColumns,
            businessAttributesFlag,
            extractFieldTagsInfo,
            extractFieldGlossaryTermsInfo,
            tv,
            entityRegistry,
        ],
    );

    // A header drag writes display.width into the ad hoc definition (synthesizing the built-in
    // layout first when no view is active); Save / Update on the Columns control persists it.
    const patchColumnDisplay = useCallback(
        (columnId: string, patch: { width?: number | null }) => {
            const base = (activeDefinition?.columns as ColumnLike[] | undefined) ?? legacyDefaultColumns(columnSources);
            const columns = base.map((c) => (columnIdentity(c) === columnId ? withDisplay(c, patch) : c));
            setAdHocDefinition({
                ...(activeDefinition || { sort: null, filter: null }),
                columns,
            } as NonNullable<Parameters<typeof setAdHocDefinition>[0]>);
        },
        [activeDefinition, columnSources, setAdHocDefinition],
    );
    const headerResize = useMemo(
        () => ({
            onResizeEnd: (columnId: string, width: number) => patchColumnDisplay(columnId, { width }),
            onReset: (columnId: string) => patchColumnDisplay(columnId, { width: null }),
            hint: tv('columnViews.resizeHint'),
        }),
        [patchColumnDisplay, tv],
    );

    const allColumns = useMemo(
        () => resolveSchemaTableColumns(columnSources, activeDefinition, headerResize),
        [columnSources, activeDefinition, headerResize],
    );
    // `sortOrder` mirrors schemaSorter so a programmatic (view default) sort is applied by antd and
    // shown by the header arrow. With no sort every column carries `null`, which renders as before.
    const finalColumns = useMemo(() => {
        const columns = visibleColumns
            ? allColumns.filter((column) => column.key && visibleColumns?.includes(column.key.toString()))
            : allColumns;
        return columns.map((column) =>
            column.sorter
                ? { ...column, sortOrder: column.key === schemaSorter?.columnKey ? schemaSorter?.order ?? null : null }
                : column,
        );
    }, [allColumns, visibleColumns, schemaSorter]);

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

    useEffect(() => {
        if (filterText === '') {
            setIsSearchActive(false);
        } else setIsSearchActive(true);
    }, [filterText]);

    useEffect(() => {
        setExpandedRows((previousRows) => {
            const finalRowsSet = new Set();
            expandedRowsFromFilter.forEach((row) => finalRowsSet.add(row));
            previousRows.forEach((row) => finalRowsSet.add(row));
            return finalRowsSet as Set<string>;
        });
    }, [expandedRowsFromFilter]);

    const [VT, setVT, vtRef] = useVT(() => ({ scroll: { y: tableHeight } }), [tableHeight]);
    const tableRef = useRef<HTMLDivElement>(null);

    useEffect(() => setVT({ body: { row: SchemaRow } }), [setVT]);

    // Keep useVT's virtualized body; add the resizable header cell on top of it.
    const tableComponents = useMemo(
        () => ({ ...VT, header: { ...((VT as { header?: object }).header || {}), cell: ResizableHeaderCell } }),
        [VT],
    );

    useDebounce(
        () => {
            if (!expandedDrawerFieldPath) return;

            if (tableRef.current) {
                const tableBody = tableRef.current.querySelector('.ant-table-body');
                const row = tableBody?.querySelector(`[data-row-key="${CSS.escape(expandedDrawerFieldPath)}"]`);
                if (row) {
                    row.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
                }
            }
            // only scroll to new row on arrow key, navigate from header click or initial load
        },
        KEYBOARD_CONTROL_DEBOUNCE_MS,
        [expandedDrawerFieldPath, tableRef, filterText, schemaSorter],
    );

    const [shouldScrollToSelectedRow, setShouldScrollToSelectedRow] = useState(true);

    // scroll to expanded field on page load
    useEffect(() => {
        if (expandedDrawerFieldPath && shouldScrollToSelectedRow) {
            const indexToScrollTo = findIndexOfFieldPathExcludingCollapsedFields(
                expandedDrawerFieldPath,
                expandedRows,
                rows,
                schemaSorter,
                finalColumns.find((column) => column.key === schemaSorter?.columnKey)?.sorter as any,
            );
            if (indexToScrollTo >= 0) {
                setShouldScrollToSelectedRow?.(false);
                vtRef?.current?.scrollToIndex(indexToScrollTo);
            }
        }
        /* eslint-disable-next-line react-hooks/exhaustive-deps */
    }, [expandedRows, expandedDrawerFieldPath, finalColumns]);

    const rowClassName = (record) => {
        let className = '';

        if (expandedDrawerFieldPath === record.fieldPath) {
            className += 'selected-row';
        }
        if (expandedRows.has(record?.fieldPath)) {
            className += ' expanded-row';
        }
        // Add different classes based on depth
        if (record?.depth < 2) className += ` level-${record?.depth}`;
        else className += ' level-n';

        const path: string = record?.fieldPath?.toString();

        expandedRows.forEach((row) => {
            if (path.startsWith(`${row}.`)) {
                className += ' expanded-child';
            }
        });

        return className;
    };

    const hasSomeRowsWithDepthGreaterThanZero = useMemo(() => rows.some((row) => row.depth || 0 > 1), [rows]);

    const [schemaFieldDrawerFieldPath, setSchemaFieldDrawerFieldPath] = useState(expandedDrawerFieldPath);
    useDebounce(() => setSchemaFieldDrawerFieldPath(expandedDrawerFieldPath), KEYBOARD_CONTROL_DEBOUNCE_MS, [
        expandedDrawerFieldPath,
    ]);

    // Column View row filter applied; identical to `rows` when no filter is active.
    const dataSource = viewFilter.rows;
    const [sortedDataSource, setSortedDataSource] = useState(dataSource);

    const [displayedRows, setDisplayedRows] = useState(dataSource);
    const [sortedDisplayedRows, setSortedDisplayedRows] = useState(displayedRows);

    const { selectPreviousField, selectNextField } = useKeyboardControls(
        schemaSorter ? sortedDisplayedRows : displayedRows,
        expandedDrawerFieldPath,
        setExpandedDrawerFieldPath,
        expandedRows,
        setExpandedRows,
        vtRef?.current,
    );

    useEffect(() => {
        const updateDisplayedRows = () => {
            const visibleRows: ExtendedSchemaFields[] = [];

            const getVisibleRows = (data) => {
                data.forEach((record) => {
                    visibleRows.push(record);
                    if (expandedRows.has(record.fieldPath) && record.children) {
                        getVisibleRows(record.children);
                    }
                });
            };
            if (schemaSorter) getVisibleRows(sortedDataSource);
            else getVisibleRows(dataSource);

            setDisplayedRows(visibleRows);
            setSortedDisplayedRows(visibleRows);
        };
        updateDisplayedRows();
    }, [expandedRows, dataSource, sortedDataSource, schemaSorter]);

    const sortData = (data, sorter) => {
        if (sorter.order) {
            const { field, order } = sorter;

            const column = finalColumns.find((col) => col.key === field);

            if (column && column.sorter) {
                const sortedRows = data.slice().sort((a, b) => {
                    const sorterFunction = typeof column.sorter === 'function' ? column.sorter : undefined;

                    return sorterFunction ? sorterFunction(a, b) : 0;
                });
                return order === 'ascend' ? sortedRows : sortedRows.reverse();
            }
        }
        return data;
    };

    const handleTableChange = (_, __, sorter, { currentDataSource }) => {
        setSchemaSorter(sorter as SorterResult<ExtendedSchemaFields>);
        setSortedDataSource(currentDataSource);
        const sortedrows = sortData(displayedRows, sorter);
        setSortedDisplayedRows(sortedrows);
    };

    // The view's default sort becomes the sorter when its content changes or another view is picked.
    // Keyed on content, not object identity, so an ad hoc change (a resize, a filter clear) does not
    // re-apply it over the user's manual sort; a switch to a view with no sort clears the sorter.
    const viewDefaultSort = useMemo(() => defaultSortForView(activeDefinition), [activeDefinition]);
    const viewDefaultSortKey = viewDefaultSort ? `${viewDefaultSort.columnKey}:${viewDefaultSort.order}` : undefined;
    const selectedColumnViewUrn = selectedColumnView?.urn;
    useEffect(() => {
        setSchemaSorter(viewDefaultSort ? (viewDefaultSort as any) : undefined);
        // antd only reports sorted rows through onChange; mirror it for keyboard navigation.
        if (viewDefaultSort) {
            setSortedDataSource(sortData(dataSource, { field: viewDefaultSort.columnKey, order: viewDefaultSort.order }));
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [viewDefaultSortKey, selectedColumnViewUrn]);

    return (
        <>
            {isViewFilterActive && (
                <ViewFilterSummary data-testid="column-view-filter-summary">
                    {tv('columnViews.showingOf', { shown: viewFilter.shown, total: viewFilter.total })} ·{' '}
                    <Typography.Link onClick={clearViewFilter}>{tv('columnViews.clear')}</Typography.Link>
                </ViewFilterSummary>
            )}
            <TableContainer
                ref={tableRef}
                isSearchActive={isSearchActive}
                hasRowWithDepth={hasSomeRowsWithDepthGreaterThanZero}
                data-testid="schema-table-container"
            >
                <ResizeObserver onResize={(dimensions) => setTableHeight(dimensions.height - TABLE_HEADER_HEIGHT)}>
                    <StyledTable
                        data-testid="schema-table"
                        onChange={handleTableChange}
                        rowClassName={rowClassName}
                        columns={finalColumns}
                        dataSource={dataSource}
                        rowKey="fieldPath"
                        scroll={{ x: SCROLL_X, y: tableHeight }}
                        components={tableComponents}
                        expandable={{
                            expandedRowKeys: [...Array.from(expandedRows)],
                            defaultExpandAllRows: false,
                            expandRowByClick: false,
                            expandIcon: (props) => <ExpandIcon {...props} />,
                            onExpand: (expanded, record) => {
                                if (expanded) {
                                    setExpandedRows((previousRows) => new Set(previousRows.add(record.fieldPath)));
                                } else {
                                    setExpandedRows((previousRows) => {
                                        previousRows.delete(record.fieldPath);
                                        return new Set(previousRows);
                                    });
                                }
                            },
                            indentSize: 0,
                        }}
                        pagination={false}
                        onRow={(record) => ({
                            onClick: () => {
                                // shouldScrollToSelectedRow is meant for scrolling on page load, scrolling
                                // on select for certain screen sizes causes weird UI bug
                                setShouldScrollToSelectedRow(false);
                                setExpandedDrawerFieldPath(
                                    expandedDrawerFieldPath === record.fieldPath ? null : record.fieldPath,
                                );
                            },
                            id: `column-${record.fieldPath}`,
                            'data-testid': `schema-field-${record.fieldPath}`,
                        })}
                        showSorterTooltip={false}
                    />
                </ResizeObserver>
            </TableContainer>
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
                    displayedRows={schemaSorter ? sortedDisplayedRows : displayedRows}
                    refetch={refetch}
                />
            )}
        </>
    );
}
