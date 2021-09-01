import React, { useMemo, useState } from 'react';
import { Table, Tooltip } from 'antd';
import { ColumnsType } from 'antd/es/table';
import { AlignType } from 'rc-table/lib/interface';
import styled from 'styled-components';
import { geekblue } from '@ant-design/colors';
import TypeIcon from './components/TypeIcon';
import {
    EditableSchemaMetadata,
    SchemaFieldDataType,
    GlobalTags,
    SchemaField,
    GlobalTagsUpdate,
    EditableSchemaFieldInfo,
    GlossaryTerms,
    UsageQueryResult,
} from '../../../../../types.generated';
import TagTermGroup from '../../../../shared/tags/TagTermGroup';
import DescriptionField from './components/SchemaDescriptionField';
import schemaTitleRenderer from './utils/schemaTitleRenderer';
import { diffMarkdown, pathMatchesNewPath } from './utils/utils';
import { ExtendedSchemaFields } from './utils/types';

const TableContainer = styled.div`
    & .table-red-row {
        background-color: #ffa39e99;
        &: hover > td.ant-table-cell {
            background-color: #ffa39eaa;
        }
    }
    & .table-green-row {
        background-color: #b7eb8f99;
        &: hover > td.ant-table-cell {
            background-color: #b7eb8faa;
        }
    }
    &&& .ant-table-row-indent.indent-level-0 {
        padding-left: 0px !important;
    }
    &&& .ant-table-row-indent.indent-level-1 {
        padding-left: 15px !important;
    }
    &&& .ant-table-row-indent.indent-level-2 {
        padding-left: 30px !important;
    }
    &&& .ant-table-row-indent.indent-level-3 {
        padding-left: 45px !important;
    }
    &&& .ant-table-row-indent.indent-level-4 {
        padding-left: 60px !important;
    }
    &&& .ant-table-row-indent {
        padding-left: 60px !important;
    }
`;

// indent-level-8

const UsageBar = styled.div<{ width: number }>`
    width: ${(props) => props.width}px;
    height: 10px;
    background-color: ${geekblue[3]};
    border-radius: 2px;
`;

const UsageBarContainer = styled.div`
    width: 100%;
    height: 100%;
`;

const defaultColumns = [
    {
        width: 150,
        title: 'Type',
        dataIndex: 'type',
        key: 'type',
        align: 'left' as AlignType,
        render: (type: SchemaFieldDataType, record: SchemaField) => {
            return <TypeIcon type={type} nativeDataType={record.nativeDataType} />;
        },
    },
    {
        title: 'Field',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        width: 100,
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
const USAGE_BAR_MAX_WIDTH = 50;
export default function SchemaTableLegacy({
    rows,
    onUpdateDescription,
    onUpdateTags,
    editableSchemaMetadata,
    usageStats,
    editMode = true,
}: Props) {
    const hasUsageStats = useMemo(() => (usageStats?.aggregations?.fields?.length || 0) > 0, [usageStats]);
    const maxFieldUsageCount = useMemo(
        () => Math.max(...(usageStats?.aggregations?.fields?.map((field) => field?.count || 0) || [])),
        [usageStats],
    );
    const [tagHoveredIndex, setTagHoveredIndex] = useState<string | undefined>(undefined);
    const descriptionRender = (description: string, record: ExtendedSchemaFields) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        if (!editMode && record.previousDescription) {
            return (
                <DescriptionField
                    description={diffMarkdown(record.previousDescription, description)}
                    isEdited={!!relevantEditableFieldInfo?.description}
                    onUpdate={(updatedDescription) => onUpdateDescription(updatedDescription, record)}
                    editable={editMode}
                />
            );
        }

        return (
            <DescriptionField
                description={editMode ? relevantEditableFieldInfo?.description || description : description}
                original={record.description}
                isEdited={!!relevantEditableFieldInfo?.description}
                onUpdate={(updatedDescription) => onUpdateDescription(updatedDescription, record)}
                editable={editMode}
            />
        );
    };

    const tagAndTermRender = (tags: GlobalTags, record: SchemaField, rowIndex: number | undefined) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        return (
            <TagTermGroup
                uneditableTags={tags}
                editableTags={relevantEditableFieldInfo?.globalTags}
                glossaryTerms={record.glossaryTerms as GlossaryTerms}
                canRemove
                canAdd={tagHoveredIndex === `${record.fieldPath}-${rowIndex}`}
                onOpenModal={() => setTagHoveredIndex(undefined)}
                updateTags={(update) =>
                    onUpdateTags(update, relevantEditableFieldInfo || { fieldPath: record.fieldPath })
                }
            />
        );
    };

    const usageStatsRenderer = (fieldPath: string) => {
        const relevantUsageStats = usageStats?.aggregations?.fields?.find((fieldStats) =>
            pathMatchesNewPath(fieldStats?.fieldName, fieldPath),
        );

        if (!relevantUsageStats) {
            return null;
        }

        return (
            <Tooltip placement="topLeft" title={`${relevantUsageStats.count} queries / month`}>
                <UsageBarContainer>
                    <UsageBar width={((relevantUsageStats.count || 0) / maxFieldUsageCount) * USAGE_BAR_MAX_WIDTH} />
                </UsageBarContainer>
            </Tooltip>
        );
    };

    const descriptionColumn = {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: descriptionRender,
        width: 300,
    };

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

    let allColumns: ColumnsType<ExtendedSchemaFields> = [...defaultColumns, descriptionColumn, tagAndTermColumn];

    if (hasUsageStats) {
        allColumns = [...allColumns, usageColumn];
    }

    return (
        <TableContainer>
            <Table
                bordered
                columns={allColumns}
                dataSource={rows}
                rowClassName={(record) =>
                    record.isNewRow ? 'table-green-row' : `${record.isDeletedRow ? 'table-red-row' : ''}`
                }
                rowKey="fieldPath"
                expandable={{ defaultExpandAllRows: false, expandRowByClick: false }}
                pagination={false}
            />
        </TableContainer>
    );
}
