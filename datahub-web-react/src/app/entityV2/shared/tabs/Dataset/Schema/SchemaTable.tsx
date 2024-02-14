import { ColumnsType } from 'antd/es/table';
import type { FixedType } from 'rc-table/lib/interface';
import { SorterResult } from 'antd/lib/table/interface';
import ResizeObserver from 'rc-resize-observer';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { useVT } from 'virtualizedtableforantd4';

import {
    EditableSchemaMetadata,
    ForeignKeyConstraint,
    SchemaField,
    SchemaMetadata,
    UsageQueryResult,
} from '../../../../../../types.generated';
import useSchemaTitleRenderer from '../../../../dataset/profile/schema/utils/schemaTitleRenderer';
import useSchemaTypeRenderer from '../../../../dataset/profile/schema/utils/schemaTypeRenderer';
import translateFieldPath from '../../../../dataset/profile/schema/utils/translateFieldPath';
import { ExtendedSchemaFields } from '../../../../dataset/profile/schema/utils/types';
import { findIndexOfFieldPathExcludingCollapsedFields } from '../../../../dataset/profile/schema/utils/utils';
import { StyledTable } from '../../../components/styled/StyledTable';
import { REDESIGN_COLORS } from '../../../constants';
import ExpandIcon from './components/ExpandIcon';
import SchemaFieldDrawer from './components/SchemaFieldDrawer/SchemaFieldDrawer';
import { SchemaRow } from './components/SchemaRow';
import { FkContext } from './utils/selectedFkContext';
import useDescriptionRenderer from './utils/useDescriptionRenderer';
import useTagsAndTermsRenderer from './utils/useTagsAndTermsRenderer';
import useUsageStatsRenderer from './utils/useUsageStatsRenderer';

const TableContainer = styled.div<{ isSearchActive: boolean; hasRowWithDepth: boolean }>`
    overflow: inherit;
    height: inherit;

    &&& .ant-table-tbody > tr > .ant-table-cell-with-append {
        border-right: none;
        padding: 0px;
    }

    &&& .ant-table-tbody > tr {
        // max-height: 65px !important;
        // min-height: 65px !important;
        // height: 65px !important;
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

   
    &&& .selected-row{
        background: ${REDESIGN_COLORS.BACKGROUND_PURPLE} !important;
        transition: background 2s;
    }

    &&& .selected-row * {
        color: white !important;
        transition: color 0.3s;
        .field-type {
            border-color: white !important;
        }
        .depth-container {
            background: ${REDESIGN_COLORS.WHITE} !important;
        }
        .depth-text {
            background: transparent !important;
            color: ${REDESIGN_COLORS.BACKGROUND_PURPLE} !important;
        }
        .usage-bars {
            background: ${REDESIGN_COLORS.WHITE} !important;
        }
        .row-icon-tooltip .ant-tooltip-inner {
            background: #e5eff1 !important;
            color: ${REDESIGN_COLORS.DARK_GREY} !important;
        }
        .row-icon-container svg {
            stroke: white !important;
        }
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
    shouldScrollToSelectedRow?: boolean;
    setShouldScrollToSelectedRow?: (val: boolean) => void;
    openTimelineDrawer?: boolean;
    setOpenTimelineDrawer?: any;
    showTypeAsIcons?: boolean;
    matches?: {
        path: string;
        index: number;
    }[];
};

const EMPTY_SET: Set<string> = new Set();
const TABLE_HEADER_HEIGHT = 52;

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
    shouldScrollToSelectedRow,
    setShouldScrollToSelectedRow,
    openTimelineDrawer = false,
    setOpenTimelineDrawer,
    showTypeAsIcons = true,
    matches,
}: Props): JSX.Element {
    // const hasUsageStats = useMemo(() => (usageStats?.aggregations?.fields?.length || 0) > 0, [usageStats]);
    const [tableHeight, setTableHeight] = useState(0);
    const [overflowHoverFieldPath, setOverflowHoverFieldPath] = useState<string | null>(null);
    const [selectedFkFieldPath, setSelectedFkFieldPath] = useState<null | {
        fieldPath: string;
        constraint?: ForeignKeyConstraint | null;
    }>(null);
    const [schemaSorter, setSchemaSorter] = useState<SorterResult<any> | undefined>(undefined);

    const [isSearchActive, setIsSearchActive] = useState<boolean>(false);

    const [filteredRows, setFilteredRows] = useState<Array<ExtendedSchemaFields>>([]);

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
    const schemaTitleRenderer = useSchemaTitleRenderer(
        schemaMetadata,
        setSelectedFkFieldPath,
        filterText,
        overflowHoverFieldPath,
    );
    const schemaTypeRenderer = useSchemaTypeRenderer(showTypeAsIcons);

    const fieldColumn = {
        fixed: 'left' as FixedType,
        width: 200,
        title: 'Field',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        render: schemaTitleRenderer,
        filtered: true,
        onCell: () => ({
            style: {
                whiteSpace: 'pre' as any,
                // WebkitMask: 'linear-gradient(-270deg, #736BA4 0%, rgba(115, 107, 164, 0.00) 100%)',

                // WebkitMask:
                //     expandedDrawerFieldPath === record.fieldPath
                //         ? 'linear-gradient(-90deg, rgb(60, 180, 122, 0.8), rgb(60, 180, 122, 1) 10%)'
                //         : 'linear-gradient(-90deg, rgba(0,0,0,0.8) 0%, rgba(0,0,0,1) 10%)',
            },
            onMouseEnter: (e) => {
                const element = e.nativeEvent.relatedTarget as any;
                if (element) {
                    const hasOverflowingChildren =
                        element.offsetHeight < element.scrollHeight || element.offsetWidth < element.scrollWidth;
                    if (hasOverflowingChildren) {
                        // setOverflowHoverFieldPath(record.fieldPath);
                    }
                }
            },
            onMouseLeave: () => {
                setOverflowHoverFieldPath(null);
            },
        }),
        sorter: (sourceA, sourceB) => {
            return translateFieldPath(sourceA.fieldPath).localeCompare(translateFieldPath(sourceB.fieldPath));
        },
    };

    const typeColumn = {
        width: 100,
        title: 'Type',
        dataIndex: 'type',
        key: 'type',
        render: schemaTypeRenderer,
    };
    const descriptionColumn = {
        ellipsis: true,
        className: "description-column",
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: descriptionRender,
    };

    const tagColumn = {
        width: 100,
        title: 'Tags',
        dataIndex: 'globalTags',
        key: 'tag',
        render: tagRenderer,
    };

    const termColumn = {
        width: 150,
        title: 'Glossary Terms',
        dataIndex: 'globalTags',
        key: 'tag',
        render: termRenderer,
    };

    // Function to get the count of each usageStats fieldPath
    function getCount(fieldPath: any) {
        const data: any =
            usageStats?.aggregations?.fields &&
            usageStats?.aggregations?.fields.find((field) => {
                return field?.fieldName === fieldPath;
            });
        return data && data.count;
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

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

    const flattenRows = useCallback((currentRow) => {
        const flattened = [currentRow];
        if (currentRow.children) {
            flattened.push(...currentRow.children.flatMap(flattenRows));
        }
        return flattened;
    }, []);

    const filterRows = useCallback(
        (currentRow) => {
            const isMatch = matches?.some((match) => currentRow.fieldPath === match.path);
            return isMatch ? { ...currentRow } : null;
        },
        [matches],
    );

    useEffect(() => {
        if (filterText === '') {
            setIsSearchActive(false);
            setShouldScrollToSelectedRow?.(true);
        } else setIsSearchActive(true);
    }, [filterText, setShouldScrollToSelectedRow]);

    useEffect(() => {
        const flattenedRows = rows.flatMap(flattenRows);
        const filtered = flattenedRows.flatMap(filterRows).filter(Boolean);
        filtered.forEach((row) => {
            if (row.children) {
                // eslint-disable-next-line no-param-reassign
                row.children = [];
            }
        });

        setFilteredRows(filtered);
    }, [rows, filterRows, flattenRows]);

    useEffect(() => {
        setExpandedRows((previousRows) => {
            const finalRowsSet = new Set();
            expandedRowsFromFilter.forEach((row) => finalRowsSet.add(row));
            previousRows.forEach((row) => finalRowsSet.add(row));
            return finalRowsSet as Set<string>;
        });
    }, [expandedRowsFromFilter]);

    const [VT, setVT, ref] = useVT(
        () => ({
            // onScroll: () => {
            //     // setOverflowHoverFieldPath(null);
            // },
            scroll: { y: tableHeight },
        }),
        [tableHeight],
    );

    useEffect(() => {
        if (!expandedDrawerFieldPath || !shouldScrollToSelectedRow) return;

        const indexToScrollTo = findIndexOfFieldPathExcludingCollapsedFields(
            expandedDrawerFieldPath,
            expandedRows,
            rows,
            schemaSorter,
            allColumns.find((column) => column.key === schemaSorter?.columnKey)?.sorter as any,
        );
        if (indexToScrollTo >= 0) {
            setShouldScrollToSelectedRow?.(false);
            ref?.current.scrollToIndex(indexToScrollTo);
        }
        /* eslint-disable-next-line react-hooks/exhaustive-deps */
    }, [expandedRows, shouldScrollToSelectedRow]);

    useMemo(() => setVT({ body: { row: SchemaRow } }), [setVT]);

    const rowClassName = (record) => {
        let className = record.fieldPath === selectedFkFieldPath?.fieldPath ? 'open-fk-row' : '';

        if (expandedDrawerFieldPath === record.fieldPath) {
            className += 'selected-row';
        }
        if (expandedRows.has(record?.fieldPath)) {
            className += ' expanded-row';
        }
        // Add different classes based on depth
        if (record?.depth < 2) className += ` level-${record?.depth}`;
        else className += ' level-n';

        const path: string = record?.fieldPath.toString();

        expandedRows.forEach((row) => {
            if (path.startsWith(`${row}.`)) {
                className += ' expanded-child';
            }
        });

        return className;
    };

    const hasSomeRowsWithDepthGreaterThanZero = useMemo(() => rows.some((row) => row.depth || 0 > 1), [rows]);

    return (
        <FkContext.Provider value={selectedFkFieldPath}>
            <TableContainer isSearchActive={isSearchActive} hasRowWithDepth={hasSomeRowsWithDepthGreaterThanZero}>
                <ResizeObserver onResize={(dimensions) => setTableHeight(dimensions.height - TABLE_HEADER_HEIGHT)}>
                    <StyledTable
                        onChange={(_, __, sorter) => {
                            const selectedSorter = sorter as SorterResult<any> | undefined;
                            // const selectedColumn = allColumns.find(
                            //     (column) => column.key === selectedSorter?.columnKey,
                            // );

                            setSchemaSorter(selectedSorter);
                        }}
                        rowClassName={rowClassName}
                        columns={allColumns}
                        dataSource={filterText ? filteredRows : rows}
                        // rowKey={(record) => `column-${record.fieldPath}`}
                        rowKey="fieldPath"
                        scroll={{ x: 'max-content', y: tableHeight}}
                        components={VT}
                        expandable={{
                            expandedRowKeys: [...Array.from(expandedRows)],
                            defaultExpandAllRows: false,

                            expandRowByClick: false,
                            expandIcon: (props) => <ExpandIcon {...props} expandable={!isSearchActive} />,

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
                                setExpandedDrawerFieldPath(
                                    expandedDrawerFieldPath === record.fieldPath ? null : record.fieldPath,
                                );
                            },
                            id: `column-${record.fieldPath}`,
                        })}
                    />
                </ResizeObserver>
            </TableContainer>
            {!!schemaFields && (
                <SchemaFieldDrawer
                    schemaFields={schemaFields}
                    expandedDrawerFieldPath={expandedDrawerFieldPath}
                    editableSchemaMetadata={editableSchemaMetadata}
                    setExpandedDrawerFieldPath={(newPath) => {
                        setExpandedDrawerFieldPath(newPath);
                        setShouldScrollToSelectedRow?.(true);
                    }}
                    openTimelineDrawer={openTimelineDrawer}
                    setOpenTimelineDrawer={setOpenTimelineDrawer}
                    usageStats={usageStats}
                />
            )}
        </FkContext.Provider>
    );
}
