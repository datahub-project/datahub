import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Table, Typography } from 'antd';
import { EntityType, SchemaField } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { CompactFieldIconWithTooltip } from '../../../sharedV2/icons/CompactFieldIcon';
import { REDESIGN_COLORS } from '../../shared/constants';

const MAX_ROWS = 5;

const TableContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 10px;
    .ant-table-thead > tr > th {
        background-color: transparent;
        font-weight: 700;
        font-size: 14px;
        color: ${REDESIGN_COLORS.SUBTITLE};
    }
    && .ant-table-tbody > tr > td {
        padding: 8px 5px;
        border-bottom: none;
        border-right: 1px solid ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};
    }
`;

const SeeMoreLink = styled(Link)`
    color: ${REDESIGN_COLORS.SUBTITLE};
    font-size: 12px;
    font-weight: 600;
`;

interface Props {
    urn: string;
    rows: SchemaField[];
}

export default function ChartFieldsTable({ urn, rows }: Props) {
    const entityRegistry = useEntityRegistry();
    const hasSeeMore = rows.length > MAX_ROWS;

    const nameColumn = {
        ellipsis: true,
        width: '45%',
        title: 'Name',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        filtered: true,
        render: nameRender,
    };

    const descriptionColumn = {
        ellipsis: true,
        width: '45%',
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: descriptionRender,
    };

    return (
        <TableContainer>
            <Table
                size="small"
                columns={[nameColumn, descriptionColumn]}
                dataSource={rows.slice(0, MAX_ROWS)}
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
            />
            {hasSeeMore && (
                <SeeMoreLink type="text" to={`${entityRegistry.getEntityUrl(EntityType.Chart, urn)}/Fields`}>
                    View {rows.length - MAX_ROWS} More
                </SeeMoreLink>
            )}
        </TableContainer>
    );
}

const TypeWrapper = styled.span`
    color: ${REDESIGN_COLORS.SUBTITLE};
    margin-right: 4px;
    width: 11px;
`;

const FieldPathText = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.SUBTITLE};
`;

const Description = styled(Typography.Text)`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: ${REDESIGN_COLORS.SUBTITLE};
`;

function nameRender(fieldPath: string, row: SchemaField) {
    return (
        <FieldPathText>
            <TypeWrapper>
                <CompactFieldIconWithTooltip type={row.type} nativeDataType={row.nativeDataType} />
            </TypeWrapper>
            {fieldPath}
        </FieldPathText>
    );
}

function descriptionRender(description: string) {
    return <Description>{description}</Description>;
}
