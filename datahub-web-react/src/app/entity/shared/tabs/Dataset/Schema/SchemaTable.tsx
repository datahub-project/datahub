import React, { useMemo, useState } from 'react';
import { ColumnsType } from 'antd/es/table';
import styled from 'styled-components';
import {} from 'antd';
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

const TableContainer = styled.div`
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
    showSchemaBlame: boolean;
};
export default function SchemaTable({
    rows,
    schemaMetadata,
    editableSchemaMetadata,
    usageStats,
    editMode = true,
    schemaFieldBlameList,
    showSchemaBlame,
}: Props): JSX.Element {
    const hasUsageStats = useMemo(() => (usageStats?.aggregations?.fields?.length || 0) > 0, [usageStats]);

    const [tagHoveredIndex, setTagHoveredIndex] = useState<string | undefined>(undefined);
    const [selectedFkFieldPath, setSelectedFkFieldPath] =
        useState<null | { fieldPath: string; constraint?: ForeignKeyConstraint | null }>(null);

    const descriptionRender = useDescriptionRenderer(editableSchemaMetadata);
    const usageStatsRenderer = useUsageStatsRenderer(usageStats);
    const tagRenderer = useTagsAndTermsRenderer(editableSchemaMetadata, tagHoveredIndex, setTagHoveredIndex, {
        showTags: true,
        showTerms: false,
    });
    const termRenderer = useTagsAndTermsRenderer(editableSchemaMetadata, tagHoveredIndex, setTagHoveredIndex, {
        showTags: false,
        showTerms: true,
    });
    const schemaTitleRenderer = useSchemaTitleRenderer(schemaMetadata, setSelectedFkFieldPath);
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
        title: 'Field',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        width: 250,
        render: schemaTitleRenderer,
        filtered: true,
    };

    const tagColumn = {
        width: 125,
        title: 'Tags',
        dataIndex: 'globalTags',
        key: 'tag',
        render: tagRenderer,
        onCell: onTagTermCell,
    };

    const termColumn = {
        width: 125,
        title: 'Terms',
        dataIndex: 'globalTags',
        key: 'tag',
        render: termRenderer,
        onCell: onTagTermCell,
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

    const blameColumn = {
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        width: 75,
        render(record: SchemaField) {
            return {
                props: {
                    style: { backgroundColor: ANTD_GRAY[2.5] },
                },
                children: schemaBlameRenderer(record),
            };
        },
    };

    let allColumns: ColumnsType<ExtendedSchemaFields> = [fieldColumn, descriptionColumn, tagColumn, termColumn];

    if (hasUsageStats) {
        allColumns = [...allColumns, usageColumn];
    }

    if (showSchemaBlame) {
        allColumns = [...allColumns, blameColumn];
    }

    return (
        <FkContext.Provider value={selectedFkFieldPath}>
            <TableContainer>
                <StyledTable
                    rowClassName={(record) =>
                        record.fieldPath === selectedFkFieldPath?.fieldPath ? 'open-fk-row' : ''
                    }
                    columns={allColumns}
                    dataSource={rows}
                    rowKey="fieldPath"
                    components={{
                        body: {
                            row: SchemaRow,
                        },
                    }}
                    expandable={{
                        defaultExpandAllRows: false,
                        expandRowByClick: false,
                        expandIcon: ExpandIcon,
                        indentSize: 0,
                    }}
                    pagination={false}
                />
            </TableContainer>
        </FkContext.Provider>
    );
}
