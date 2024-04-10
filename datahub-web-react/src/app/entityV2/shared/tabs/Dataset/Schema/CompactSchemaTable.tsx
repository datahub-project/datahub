import React, { useMemo } from 'react';
import { ColumnsType } from 'antd/es/table';
import { Button, Table } from 'antd';
import styled from 'styled-components';
import { FixedType } from 'rc-table/lib/interface';
import {
    EditableSchemaMetadata,
    EntityType,
    SchemaMetadata,
    UsageQueryResult,
} from '../../../../../../types.generated';
import useSchemaTitleRenderer from '../../../../dataset/profile/schema/utils/schemaTitleRenderer';
import { ExtendedSchemaFields } from '../../../../dataset/profile/schema/utils/types';
import useDescriptionRenderer from './utils/useDescriptionRenderer';
import useUsageStatsRenderer from './utils/useUsageStatsRenderer';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { REDESIGN_COLORS } from '../../../constants';
import ExpandIcon from './components/ExpandIcon';

export type Props = {
    rows: Array<ExtendedSchemaFields>;
    schemaMetadata: SchemaMetadata | undefined | null;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    usageStats?: UsageQueryResult | null;
    fullHeight?: boolean;
};

const TableContainer = styled.div<{ fullHeight?: boolean }>`
    margin-top: ${(props) => (props.fullHeight ? '0px' : '5px')};
    margin-left: ${(props) => (props.fullHeight ? '12px' : '0px')};
    .ant-table-thead > tr > th {
        background-color: transparent;
        font-weight: 600;
        color: ${REDESIGN_COLORS.DARK_GREY};
        font-weight: 700;
    }
    &&& .ant-table-cell:first-of-type {
        ${(props) => !props.fullHeight && 'padding: 8px 8px 8px 0px'};
    }
    &&& .description-column {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 100px;
    }
`;

const StyledButton = styled(Button)`
    color: #b0a2c2;
    font-weight: 500;
    :hover {
        color: ${REDESIGN_COLORS.DARK_GREY};
    }
`;

export default function CompactSchemaTable({
    rows,
    schemaMetadata,
    editableSchemaMetadata,
    usageStats,
    fullHeight,
}: Props): JSX.Element {
    const numberOfRowsToShow = fullHeight ? 20 : 5;
    const { urn } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const hasUsageStats = useMemo(() => (usageStats?.aggregations?.fields?.length || 0) > 0, [usageStats]);

    const descriptionRender = useDescriptionRenderer(editableSchemaMetadata, true);
    const usageStatsRenderer = useUsageStatsRenderer(usageStats);

    const schemaTitleRenderer = useSchemaTitleRenderer(schemaMetadata, () => {}, '', null, true);

    const shortenedRows = useMemo(() => rows.slice(0, numberOfRowsToShow), [rows, numberOfRowsToShow]);
    const hasSeeMore = rows.length > numberOfRowsToShow;

    const fieldColumn = {
        fixed: 'left' as FixedType,
        title: 'Name',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        render: schemaTitleRenderer,
        filtered: true,
        onCell: () => ({
            style: {
                whiteSpace: 'pre' as any,
                WebkitMask: 'linear-gradient(-90deg, rgba(0,0,0,0) 0%, rgba(0,0,0,1) 10%)',
            },
        }),
    };

    const descriptionColumn = {
        ellipsis: true,
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        className: 'description-column',
        render: descriptionRender,
        onCell: () => ({
            style: {
                whiteSpace: 'pre' as any,
                overflow: 'wrap',
                textWrap: 'whitespace',
                WebkitMask: 'linear-gradient(-90deg, rgba(0,0,0,0) 0%, rgba(0,0,0,1) 10%)',
            },
        }),
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
        width: '100',
        title: 'Usage',
        dataIndex: 'fieldPath',
        key: 'usage',
        render: usageStatsRenderer,
        sorter: (sourceA, sourceB) => getCount(sourceA.fieldPath) - getCount(sourceB.fieldPath),
    };

    let allColumns: ColumnsType<ExtendedSchemaFields> = [fieldColumn, descriptionColumn];

    if (hasUsageStats) {
        allColumns = [...allColumns, usageColumn];
    }

    return (
        <TableContainer fullHeight={fullHeight}>
            <Table
                size="small"
                columns={allColumns}
                dataSource={shortenedRows}
                rowKey="fieldPath"
                pagination={false}
                onRow={(record) => ({
                    style: {
                        padding: '0px',
                        maxWidth: '300px',
                        minWidth: '300px',
                    },
                    id: `column-${record.fieldPath}`,
                })}
                scroll={{ x: 'auto' }}
                expandable={{
                    expandIcon: (props) => <ExpandIcon {...props} isCompact />,
                }}
            />
            {hasSeeMore && (
                <StyledButton type="text" size="small" href={entityRegistry.getEntityUrl(EntityType.Dataset, urn)}>
                    View {rows.length - numberOfRowsToShow} More
                </StyledButton>
            )}
        </TableContainer>
    );
}
