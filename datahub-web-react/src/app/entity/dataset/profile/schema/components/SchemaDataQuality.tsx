import React, { useState } from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { ColumnsType } from 'antd/es/table';
import { ANTD_GRAY } from '../../../../shared/constants';
import { ExtendedDataQualitySchemaFields } from '../utils/types';
import { StyledTable } from '../../../../shared/components/styled/StyledTable';
import { Constraints } from '../../../../../../types.generated';
import SchemaResultMetric from './SchemaResultMetric';

export type Props = {
    dataRows: Array<ExtendedDataQualitySchemaFields>;
};
const EntityTitle = styled(Typography.Title)`
    &&& {
        margin-bottom: 0;
    }
`;
const Header = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 20px 20px 0 20px;
    flex-shrink: 0;
    min-height: 60px;
`;
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
    &&& .ant-table-tbody > tr {
        cursor: pointer;
    }
`;
const ResultMetricContainer = styled.div`
    padding: 20px 0;
`;
export default function SchemaDataQuality({ dataRows }: Props) {
    const [showResultMetric, setShowResultMetric] = useState(false);
    const [fieldName, setFieldName] = useState('');
    const [resultMetricData, setResultMetricData] = useState<Array<Constraints>>([]);
    const fieldColumn = {
        title: 'Field',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        width: 100,
    };

    const constraintColumn = {
        width: 250,
        title: 'Constraints',
        dataIndex: 'constraints',
        key: 'constraints',
        render: (item) => (
            <span>
                {item.constraints.map((e, i) => `${e.constraint} ${i === item.constraints.length - 1 ? '' : '| '}`)}
            </span>
        ),
    };

    const allColumns: ColumnsType<ExtendedDataQualitySchemaFields> = [fieldColumn, constraintColumn];
    if (dataRows) {
        dataRows.map((element) => {
            /* eslint-disable no-param-reassign */
            element.constraints = {
                constraints: [
                    {
                        constraint: 'expect_column_value_to_be_null',
                        resultMetrics: [
                            {
                                timestamp: '1 Jan 2022',
                                elementCount: 10,
                                unexpectedCount: 5,
                                unexpectedPercent: 2,
                                missingCound: 2,
                            },
                            {
                                timestamp: '2 Jan 2022',
                                elementCount: 8,
                                unexpectedCount: 6,
                                unexpectedPercent: 3,
                                missingCound: 5,
                            },
                            {
                                timestamp: '3 Jan 2022',
                                elementCount: 12,
                                unexpectedCount: 7,
                                unexpectedPercent: 5,
                                missingCound: 3,
                            },
                            {
                                timestamp: '4 Jan 2022',
                                elementCount: 13,
                                unexpectedCount: 3,
                                unexpectedPercent: 12,
                                missingCound: 6,
                            },
                            {
                                timestamp: '5 Jan 2022',
                                elementCount: 7,
                                unexpectedCount: 7,
                                unexpectedPercent: 15,
                                missingCound: 8,
                            },
                            {
                                timestamp: '6 Jan 2022',
                                elementCount: 10,
                                unexpectedCount: 4,
                                unexpectedPercent: 10,
                                missingCound: 5,
                            },
                        ],
                    },
                    {
                        constraint: 'expect_column_value_to_be_unique',
                        resultMetrics: [
                            {
                                timestamp: '1 Jan 2022',
                                elementCount: 8,
                                unexpectedCount: 3,
                                unexpectedPercent: 5,
                                missingCound: 6,
                            },
                            {
                                timestamp: '2 Jan 2022',
                                elementCount: 10,
                                unexpectedCount: 12,
                                unexpectedPercent: 7,
                                missingCound: 15,
                            },
                            {
                                timestamp: '3 Jan 2022',
                                elementCount: 8,
                                unexpectedCount: 12,
                                unexpectedPercent: 9,
                                missingCound: 6,
                            },
                            {
                                timestamp: '4 Jan 2022',
                                elementCount: 2,
                                unexpectedCount: 12,
                                unexpectedPercent: 9,
                                missingCound: 3,
                            },
                            {
                                timestamp: '5 Jan 2022',
                                elementCount: 3,
                                unexpectedCount: 12,
                                unexpectedPercent: 13,
                                missingCound: 3,
                            },
                            {
                                timestamp: '6 Jan 2022',
                                elementCount: 3,
                                unexpectedCount: 5,
                                unexpectedPercent: 6,
                                missingCound: 15,
                            },
                        ],
                    },
                ],
            };
            /* eslint-enable no-param-reassign */
            return element;
        });
    }

    const onRowClickFunction = (record) => {
        return {
            onClick: () => {
                setShowResultMetric(true);
                setFieldName(record.fieldPath);
                setResultMetricData(record.constraints.constraints);
            },
        };
    };
    return (
        <div>
            <Header>
                <EntityTitle level={5}>Constraints</EntityTitle>
            </Header>
            <TableContainer>
                <StyledTable
                    columns={allColumns}
                    dataSource={dataRows}
                    rowKey="fieldPath"
                    pagination={false}
                    onRow={onRowClickFunction}
                />
            </TableContainer>
            {showResultMetric && (
                <ResultMetricContainer>
                    <Header>
                        <EntityTitle level={5}>Result Metrics: Selected Column- {fieldName}</EntityTitle>
                    </Header>

                    <SchemaResultMetric data={resultMetricData} />
                </ResultMetricContainer>
            )}
        </div>
    );
}
