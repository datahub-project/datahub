import React, { useMemo, useState } from 'react';
import { ColumnsType } from 'antd/es/table';
import styled from 'styled-components';
import {
    EditableSchemaMetadata,
    SchemaField,
    GlobalTagsUpdate,
    EditableSchemaFieldInfo,
    UsageQueryResult,
} from '../../../../../../types.generated';
import schemaTitleRenderer from '../../../../dataset/profile/schema/utils/schemaTitleRenderer';
import { ExtendedSchemaFields } from '../../../../dataset/profile/schema/utils/types';
import useDescriptionRenderer from './utils/useDescriptionRenderer';
import useUsageStatsRenderer from './utils/useUsageStatsRenderer';
import useTagsAndTermsRenderer from './utils/useTagsAndTermsRenderer';
import ExpandIcon from './components/ExpandIcon';
import { StyledTable } from '../../../components/styled/StyledTable';

const TableContainer = styled.div`
    &&& .ant-table-tbody > tr > .ant-table-cell-with-append {
        border-right: none;
        padding: 0px;
    }

    &&& .ant-table-tbody > tr > .ant-table-cell {
        border-right: none;
    }
`;

const defaultColumns = [
    {
        title: 'Field',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        width: 250,
        render: schemaTitleRenderer,
        filtered: true,
    },
];

export type Props = {
    rows: Array<ExtendedSchemaFields>;
    onUpdateDescription: (
        updatedDescription: string,
        record?: EditableSchemaFieldInfo | ExtendedSchemaFields,
    ) => Promise<any>;
    onUpdateTags: (update: GlobalTagsUpdate, record?: EditableSchemaFieldInfo) => Promise<any>;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    editMode?: boolean;
    usageStats?: UsageQueryResult | null;
};
export default function SchemaTable({
    rows,
    onUpdateDescription,
    onUpdateTags,
    editableSchemaMetadata,
    usageStats,
    editMode = true,
}: Props) {
    const hasUsageStats = useMemo(() => (usageStats?.aggregations?.fields?.length || 0) > 0, [usageStats]);

    const [tagHoveredIndex, setTagHoveredIndex] = useState<string | undefined>(undefined);

    const descriptionRender = useDescriptionRenderer(editableSchemaMetadata, onUpdateDescription);
    const usageStatsRenderer = useUsageStatsRenderer(usageStats);
    const tagAndTermRender = useTagsAndTermsRenderer(
        editableSchemaMetadata,
        onUpdateTags,
        tagHoveredIndex,
        setTagHoveredIndex,
    );

    const tagAndTermColumn = {
        width: 150,
        title: 'Tags & Terms',
        dataIndex: 'globalTags',
        key: 'tag',
        render: tagAndTermRender,
        onCell: (record: SchemaField, rowIndex: number | undefined) => ({
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
        }),
    };

    const usageColumn = {
        width: 50,
        title: 'Usage',
        dataIndex: 'fieldPath',
        key: 'usage',
        render: usageStatsRenderer,
    };
    const descriptionColumn = {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: descriptionRender,
        width: 300,
    };

    let allColumns: ColumnsType<ExtendedSchemaFields> = [...defaultColumns, descriptionColumn, tagAndTermColumn];

    if (hasUsageStats) {
        allColumns = [...allColumns, usageColumn];
    }

    return (
        <TableContainer>
            <StyledTable
                columns={allColumns}
                dataSource={rows}
                rowKey="fieldPath"
                expandable={{
                    defaultExpandAllRows: false,
                    expandRowByClick: false,
                    expandIcon: ExpandIcon,
                    indentSize: 0,
                }}
                pagination={false}
            />
        </TableContainer>
    );
}
