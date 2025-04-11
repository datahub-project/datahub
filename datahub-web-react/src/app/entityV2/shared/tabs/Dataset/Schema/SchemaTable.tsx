import { ColumnsType } from 'antd/es/table';
import type { FixedType } from 'rc-table/lib/interface';
import { SorterResult } from 'antd/lib/table/interface';
import ResizeObserver from 'rc-resize-observer';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';
import { useVT } from 'virtualizedtableforantd4';
import { useDebounce } from 'react-use';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { colors } from '@src/alchemy-components';
import {
    EditableSchemaMetadata,
    SchemaField,
    SchemaMetadata,
    UsageQueryResult,
} from '../../../../../../types.generated';
import SchemaRow from '../../../../dataset/profile/schema/components/SchemaRow';
import useSchemaTitleRenderer from '../../../../dataset/profile/schema/utils/schemaTitleRenderer';
import useSchemaTypeRenderer from '../../../../dataset/profile/schema/utils/schemaTypeRenderer';
import translateFieldPath from '../../../../dataset/profile/schema/utils/translateFieldPath';
import { ExtendedSchemaFields } from '../../../../dataset/profile/schema/utils/types';
import { StyledTable } from '../../../components/styled/StyledTable';
import { REDESIGN_COLORS } from '../../../constants';
import ExpandIcon from './components/ExpandIcon';
import SchemaFieldDrawer from './components/SchemaFieldDrawer/SchemaFieldDrawer';
import useDescriptionRenderer from './utils/useDescriptionRenderer';
import useTagsAndTermsRenderer from './utils/useTagsAndTermsRenderer';
import useUsageStatsRenderer from './utils/useUsageStatsRenderer';
import useKeyboardControls from './useKeyboardControls';
import { findIndexOfFieldPathExcludingCollapsedFields } from '../../../../dataset/profile/schema/utils/utils';
import useExtractFieldGlossaryTermsInfo from './utils/useExtractFieldGlossaryTermsInfo';
import useExtractFieldTagsInfo from './utils/useExtractFieldTagsInfo';
import useExtractFieldDescriptionInfo from './utils/useExtractFieldDescriptionInfo';
import { useGetTableColumnProperties } from './utils/useGetTableColumnProperties';
import { useGetStructuredPropColumns } from './utils/useGetStructuredPropColumns';

const TableContainer = styled.div<{ isSearchActive: boolean; hasRowWithDepth: boolean }>`
    overflow: inherit;
    height: inherit;

    &&& .ant-table-tbody > tr > .ant-table-cell-with-append {
        border-right: none;
        padding: 0px;
    }

    &&& .ant-table-tbody > tr {
        background-color: #fff;
    }

    &&& .ant-table-tbody > tr.expanded-child {
        background-color: #f5f9fa;
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
            background-color: ${REDESIGN_COLORS.HEADING_COLOR} !important;
        }

        .row-icon-tooltip .ant-tooltip-inner {
            background: #e5eff1 !important;
            color: ${REDESIGN_COLORS.DARK_GREY} !important;
        }

        .ant-tag {
            background-color: ${REDESIGN_COLORS.WHITE};
        }
    }

    &&& .selected-row {
        background: ${colors.gray[100]} !important;
    }

    &&& .level-0 td .row-icon-container .row-icon {
        ${(props) => (props.isSearchActive && props.hasRowWithDepth ? '' : `display: none;`)}
    }

    &&& .level-1 td .row-icon-container .row-icon {
        ${(props) => (props.isSearchActive && props.hasRowWithDepth ? '' : `display: none;`)}
    }

    &&& tr.expanded-row td:first-of-type {
        border-left: ${(props) =>
            props.isSearchActive ? '4px solid #ffffff00' : `4px solid ${REDESIGN_COLORS.BACKGROUND_PURPLE}`};
    }

    &&& .expanded-child > td {
        .depth-container {
            background: ${REDESIGN_COLORS.PRIMARY_PURPLE};
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

export type Props = {
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
};

const EMPTY_SET: Set<string> = new Set();
const TABLE_HEADER_HEIGHT = 52;
const KEYBOARD_CONTROL_DEBOUNCE_MS = 50;

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
}: Props): JSX.Element {
    const { urn: entityUrn } = useEntityData();

    const [tableHeight, setTableHeight] = useState(0);
    const [schemaSorter, setSchemaSorter] = useState<SorterResult<any> | undefined>(undefined);

    const [isSearchActive, setIsSearchActive] = useState<boolean>(false);

    const schemaFields = schemaMetadata ? schemaMetadata.fields : inputFields;

    const [rowDescriptionExpanded, setRowDescriptionExpanded] = useState<{
        [_: string]: boolean;
    }>({});

    const handleShowMore = (field) => {
        setRowDescriptionExpanded({ [field]: true });
    };

    const descriptionRender = useDescriptionRenderer(editableSchemaMetadata, false, {
        handleShowMore,
    });
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
    const schemaTitleRenderer = useSchemaTitleRenderer(entityUrn, schemaMetadata, filterText);
    const schemaTypeRenderer = useSchemaTypeRenderer();

    const tableColumnStructuredProps = useGetTableColumnProperties();
    const structuredPropColumns = useGetStructuredPropColumns(tableColumnStructuredProps);

    const fieldColumn = {
        fixed: 'left' as FixedType,
        width: 200,
        title: 'Name',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        render: schemaTitleRenderer,
        filtered: true,
        onCell: () => ({ style: { whiteSpace: 'pre' } }),
        sorter: (sourceA, sourceB) =>
            translateFieldPath(sourceA.fieldPath).localeCompare(translateFieldPath(sourceB.fieldPath)),
    };

    const typeColumn = {
        width: 100,
        title: 'Type',
        dataIndex: 'type',
        key: 'type',
        render: schemaTypeRenderer,
        sorter: (sourceA, sourceB) => sourceA.type.localeCompare(sourceB.type),
    };

    const descriptionColumn = {
        ellipsis: true,
        className: 'description-column',
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: descriptionRender,
        sorter: (sourceA, sourceB) =>
            (extractFieldDescription(sourceA).sanitizedDescription ? 1 : 0) -
            (extractFieldDescription(sourceB).sanitizedDescription ? 1 : 0),
    };

    const tagColumn = {
        width: 100,
        title: 'Tags',
        dataIndex: 'globalTags',
        key: 'tag',
        render: tagRenderer,
        sorter: (sourceA, sourceB) =>
            extractFieldTagsInfo(sourceA).numberOfTags - extractFieldTagsInfo(sourceB).numberOfTags,
    };

    const termColumn = {
        width: 200,
        title: 'Glossary Terms',
        dataIndex: 'globalTags',
        key: 'term',
        render: termRenderer,
        sorter: (sourceA, sourceB) =>
            extractFieldGlossaryTermsInfo(sourceA).numberOfTerms - extractFieldGlossaryTermsInfo(sourceB).numberOfTerms,
    };

    // Function to get the count of each usageStats fieldPath
    function getCount(fieldPath: any) {
        const data: any =
            usageStats?.aggregations?.fields &&
            usageStats?.aggregations?.fields?.find((field) => {
                return field?.fieldName === fieldPath;
            });
        return (data && data.count) ?? 0;
    }

    const usageColumn = {
        width: 100,
        title: 'Stats',
        dataIndex: 'fieldPath',
        key: 'usage',
        render: usageStatsRenderer,
        sorter: (sourceA, sourceB) => getCount(sourceA.fieldPath) - getCount(sourceB.fieldPath),
    };

    const allColumns: ColumnsType<ExtendedSchemaFields> = [
        fieldColumn,
        typeColumn,
        descriptionColumn,
        tagColumn,
        termColumn,
        usageColumn,
    ];

    if (structuredPropColumns) allColumns.splice(allColumns?.length - 1, 0, ...structuredPropColumns);

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

    useDebounce(
        () => {
            if (!expandedDrawerFieldPath) return;

            if (tableRef.current) {
                const tableBody = tableRef.current.querySelector('.ant-table-body');
                const row = tableBody?.querySelector(`[data-row-key="${expandedDrawerFieldPath}"]`);
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
                allColumns.find((column) => column.key === schemaSorter?.columnKey)?.sorter as any,
            );
            if (indexToScrollTo >= 0) {
                setShouldScrollToSelectedRow?.(false);
                vtRef?.current?.scrollToIndex(indexToScrollTo);
            }
        }
        /* eslint-disable-next-line react-hooks/exhaustive-deps */
    }, [expandedRows, expandedDrawerFieldPath]);

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

    const dataSource = rows;
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

            const column = allColumns.find((col) => col.key === field);

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

    return (
        <>
            <TableContainer
                ref={tableRef}
                isSearchActive={isSearchActive}
                hasRowWithDepth={hasSomeRowsWithDepthGreaterThanZero}
            >
                <ResizeObserver onResize={(dimensions) => setTableHeight(dimensions.height - TABLE_HEADER_HEIGHT)}>
                    <StyledTable
                        onChange={handleTableChange}
                        rowClassName={rowClassName}
                        columns={allColumns}
                        dataSource={dataSource}
                        // rowKey={(record) => `column-${record.fieldPath}`}
                        rowKey="fieldPath"
                        scroll={{ x: 'max-content', y: tableHeight }}
                        components={VT}
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
                    isShowMoreEnabled={
                        (schemaFieldDrawerFieldPath && rowDescriptionExpanded[schemaFieldDrawerFieldPath]) || false
                    }
                />
            )}
        </>
    );
}
