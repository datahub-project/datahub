import React from 'react';
import { Col, Row } from 'antd';
import { ColumnsType } from 'antd/es/table';
import styled from 'styled-components';
import { StyledTable } from '../../../../shared/components/styled/StyledTable';
import { Constraints } from '../utils/types';
import SchemaResultMetricChart from './SchemaResultMetricChart';

export type Props = {
    data: Array<Constraints>;
};
const ResultMetric = styled.div`
    padding: 20px 0;
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
`;
const TableWrapper = styled.div`
    padding: 15px 20px 0 20px;
`;
const Title = styled.span`
    font-weight: 600;
    font-size: 12px;
    color: #595959;
`;
export default function SchemaResultMetric({ data }: Props) {
    const constraintColumn = {
        title: 'Constraint',
        dataIndex: 'constraint',
        key: 'constraint',
        width: 100,
        render: (item) => <span>{item}</span>,
    };

    const ResutMetricColumn = {
        width: 250,
        title: 'Result Metrics',
        dataIndex: 'resultMetrics',
        key: 'resultMetrics',
        render: (item) => (
            <span>
                {item &&
                    item.map((e) => {
                        return (
                            <Row key={e.timestamp}>
                                <Col xs={8} sm={8} md={8} lg={8} xl={8}>{`Timestamp: ${e.timestamp}`}</Col>
                                <Col xs={14} sm={14} md={14} lg={14} xl={14}>
                                    {`Element Count: ${e.elementCount} | Unexpected Count: ${e.unexpectedCount} | 
                                    Unexpected Percent: ${e.unexpectedPercent}% | Missing Count: ${e.missingCount}`}
                                </Col>
                            </Row>
                        );
                    })}
            </span>
        ),
    };

    const allColumns: ColumnsType<Constraints> = [constraintColumn, ResutMetricColumn];
    return (
        <ResultMetric>
            <Row>
                <Col xs={24} sm={24} md={24} lg={12} xl={12}>
                    <TableWrapper>
                        <Title>Result Metric Table View:</Title>
                        <TableContainer>
                            <StyledTable
                                columns={allColumns}
                                dataSource={data}
                                rowKey="resultMetric"
                                pagination={false}
                            />
                        </TableContainer>
                    </TableWrapper>
                </Col>
                <Col xs={24} sm={24} md={24} lg={12} xl={12}>
                    <SchemaResultMetricChart />
                </Col>
            </Row>
        </ResultMetric>
    );
}
