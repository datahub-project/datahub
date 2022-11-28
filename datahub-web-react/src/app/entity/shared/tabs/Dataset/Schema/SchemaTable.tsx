import React, { useEffect, useMemo, useState } from 'react';
import { ColumnsType } from 'antd/es/table';
import { useVT } from 'virtualizedtableforantd4';
import ResizeObserver from 'rc-resize-observer';
import styled from 'styled-components';
import {
    EditableSchemaMetadata,
    ForeignKeyConstraint,
    SchemaField,
    SchemaFieldBlame,
    SchemaMetadata,
    UsageQueryResult,
} from '../../../../../../types.generated';
import useSchemaTitleRenderer from '../../../../dataset/profile/schema/utils/schemaTitleRenderer';
import { ExtendedSchemaFields } from '../../../../dataset/profile/schema/utils/types';
import useDescriptionRenderer from './utils/useDescriptionRenderer';
import useUsageStatsRenderer from './utils/useUsageStatsRenderer';
import useTagsAndTermsRenderer from './utils/useTagsAndTermsRenderer';
import ExpandIcon from './components/ExpandIcon';
import { StyledTable } from '../../../components/styled/StyledTable';
import { SchemaRow } from './components/SchemaRow';
import { FkContext } from './utils/selectedFkContext';
import useSchemaBlameRenderer from './utils/useSchemaBlameRenderer';
import { ANTD_GRAY } from '../../../constants';
import MenuColumn from './components/MenuColumn';
import translateFieldPath from '../../../../dataset/profile/schema/utils/translateFieldPath';

const TableContainer = styled.div`
    overflow: inherit;
    height: inherit;

    &&& .ant-table-tbody > tr > .ant-table-cell-with-append {
        border-right: none;
        padding: 0px;
    }

    &&& .ant-table-tbody > tr > .ant-table-cell {
        border-right: none;
    }
    &&& .open-fk-row > td {
        padding-bottom: 600px;
        vertical-align: top;
    }
`;

export type Props = {
    rows: Array<ExtendedSchemaFields>;
    schemaMetadata: SchemaMetadata | undefined | null;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    editMode?: boolean;
    usageStats?: UsageQueryResult | null;
    schemaFieldBlameList?: Array<SchemaFieldBlame> | null;
    showSchemaAuditView: boolean;
    expandedRowsFromFilter?: Set<string>;
    filterText?: string;
};

const EMPTY_SET: Set<string> = new Set();
const TABLE_HEADER_HEIGHT = 52;

export default function SchemaTable({
    rows,
    schemaMetadata,
    editableSchemaMetadata,
    usageStats,
    editMode = true,
    schemaFieldBlameList,
    showSchemaAuditView,
    expandedRowsFromFilter = EMPTY_SET,
    filterText = '',
}: Props): JSX.Element {
    const hasUsageStats = useMemo(() => (usageStats?.aggregations?.fields?.length || 0) > 0, [usageStats]);
    const [tableHeight, setTableHeight] = useState(0);
    const [tagHoveredIndex, setTagHoveredIndex] = useState<string | undefined>(undefined);
    const [selectedFkFieldPath, setSelectedFkFieldPath] =
        useState<null | { fieldPath: string; constraint?: ForeignKeyConstraint | null }>(null);

    const descriptionRender = useDescriptionRenderer(editableSchemaMetadata);
    const usageStatsRenderer = useUsageStatsRenderer(usageStats);
    const tagRenderer = useTagsAndTermsRenderer(
        editableSchemaMetadata,
        tagHoveredIndex,
        setTagHoveredIndex,
        {
            showTags: true,
            showTerms: false,
        },
        filterText,
    );
    const termRenderer = useTagsAndTermsRenderer(
        editableSchemaMetadata,
        tagHoveredIndex,
        setTagHoveredIndex,
        {
            showTags: false,
            showTerms: true,
        },
        filterText,
    );
    const schemaTitleRenderer = useSchemaTitleRenderer(schemaMetadata, setSelectedFkFieldPath, filterText);
    const schemaBlameRenderer = useSchemaBlameRenderer(schemaFieldBlameList);

    const onTagTermCell = (record: SchemaField, rowIndex: number | undefined) => ({
        onMouseEnter: () => {
            if (editMode) {
                setTagHoveredIndex(`${record.fieldPath}-${rowIndex}`);
            }
        },
        onMouseLeave: () => {
            if (editMode) {
                setTagHoveredIndex(undefined);
            }
        },
    });

    const fieldColumn = {
        width: '22%',
        title: 'Field',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        render: schemaTitleRenderer,
        filtered: true,
        sorter: (sourceA, sourceB) => {
            return translateFieldPath(sourceA.fieldPath).localeCompare(translateFieldPath(sourceB.fieldPath));
        },
    };

    const descriptionColumn = {
        width: '22%',
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: descriptionRender,
    };

    const tagColumn = {
        width: '13%',
        title: 'Tags',
        dataIndex: 'globalTags',
        key: 'tag',
        render: tagRenderer,
        onCell: onTagTermCell,
    };

    const termColumn = {
        width: '13%',
        title: 'Glossary Terms',
        dataIndex: 'globalTags',
        key: 'tag',
        render: termRenderer,
        onCell: onTagTermCell,
    };

    const blameColumn = {
        width: '10%',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        render(record: SchemaField) {
            return {
                props: {
                    style: { backgroundColor: ANTD_GRAY[2.5] },
                },
                children: schemaBlameRenderer(record),
            };
        },
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
        width: '10%',
        title: 'Usage',
        dataIndex: 'fieldPath',
        key: 'usage',
        render: usageStatsRenderer,
        sorter: (sourceA, sourceB) => getCount(sourceA.fieldPath) - getCount(sourceB.fieldPath),
    };

    const menuColumn = {
        width: '5%',
        title: '',
        dataIndex: '',
        key: 'menu',
        render: (field: SchemaField) => <MenuColumn field={field} />,
    };

    let allColumns: ColumnsType<ExtendedSchemaFields> = [fieldColumn, descriptionColumn, tagColumn, termColumn];

    if (hasUsageStats) {
        allColumns = [...allColumns, usageColumn];
    }

    if (showSchemaAuditView) {
        allColumns = [...allColumns, blameColumn];
    }

    allColumns = [...allColumns, menuColumn];

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

    useEffect(() => {
        setExpandedRows((previousRows) => {
            const finalRowsSet = new Set();
            expandedRowsFromFilter.forEach((row) => finalRowsSet.add(row));
            previousRows.forEach((row) => finalRowsSet.add(row));
            return finalRowsSet as Set<string>;
        });
    }, [expandedRowsFromFilter]);

    const [VT, setVT] = useVT(() => ({ scroll: { y: tableHeight } }), [tableHeight]);

    useMemo(() => setVT({ body: { row: SchemaRow } }), [setVT]);

    return (
        <FkContext.Provider value={selectedFkFieldPath}>
            <TableContainer>
                <ResizeObserver onResize={(dimensions) => setTableHeight(dimensions.height - TABLE_HEADER_HEIGHT)}>
                    <StyledTable
                        rowClassName={(record) =>
                            record.fieldPath === selectedFkFieldPath?.fieldPath ? 'open-fk-row' : ''
                        }
                        columns={allColumns}
                        dataSource={rows}
                        rowKey="fieldPath"
                        scroll={{ y: tableHeight }}
                        components={VT}
                        expandable={{
                            expandedRowKeys: [...Array.from(expandedRows)],
                            defaultExpandAllRows: false,
                            expandRowByClick: false,
                            expandIcon: ExpandIcon,
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
                    />
                </ResizeObserver>
            </TableContainer>
        </FkContext.Provider>
    );
}
