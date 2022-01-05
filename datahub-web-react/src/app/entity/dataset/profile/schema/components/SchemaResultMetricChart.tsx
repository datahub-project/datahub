import React, { useState } from 'react';
import styled from 'styled-components';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import { Select } from 'antd';

const { Option } = Select;
const missingCount = [
    { date: '1 Jan', expect_column_value_to_be_null: 2, expect_column_value_to_be_unique: 6 },
    { date: '2 Jan', expect_column_value_to_be_null: 5, expect_column_value_to_be_unique: 15 },
    { date: '3 Jan', expect_column_value_to_be_null: 3, expect_column_value_to_be_unique: 6 },
    { date: '4 Jan', expect_column_value_to_be_null: 6, expect_column_value_to_be_unique: 3 },
    { date: '5 Jan', expect_column_value_to_be_null: 8, expect_column_value_to_be_unique: 3 },
    { date: '6 Jan', expect_column_value_to_be_null: 5, expect_column_value_to_be_unique: 15 },
];
const elementCount = [
    { date: '1 Jan', expect_column_value_to_be_null: 10, expect_column_value_to_be_unique: 8 },
    { date: '2 Jan', expect_column_value_to_be_null: 8, expect_column_value_to_be_unique: 10 },
    { date: '3 Jan', expect_column_value_to_be_null: 12, expect_column_value_to_be_unique: 8 },
    { date: '4 Jan', expect_column_value_to_be_null: 13, expect_column_value_to_be_unique: 2 },
    { date: '5 Jan', expect_column_value_to_be_null: 7, expect_column_value_to_be_unique: 3 },
    { date: '6 Jan', expect_column_value_to_be_null: 10, expect_column_value_to_be_unique: 3 },
];
const unexpectedCount = [
    { date: '1 Jan', expect_column_value_to_be_null: 5, expect_column_value_to_be_unique: 3 },
    { date: '2 Jan', expect_column_value_to_be_null: 6, expect_column_value_to_be_unique: 12 },
    { date: '3 Jan', expect_column_value_to_be_null: 7, expect_column_value_to_be_unique: 12 },
    { date: '4 Jan', expect_column_value_to_be_null: 3, expect_column_value_to_be_unique: 12 },
    { date: '5 Jan', expect_column_value_to_be_null: 7, expect_column_value_to_be_unique: 12 },
    { date: '6 Jan', expect_column_value_to_be_null: 4, expect_column_value_to_be_unique: 5 },
];
const unexpectedPercent = [
    { date: '1 Jan', expect_column_value_to_be_null: 2, expect_column_value_to_be_unique: 5 },
    { date: '2 Jan', expect_column_value_to_be_null: 3, expect_column_value_to_be_unique: 7 },
    { date: '3 Jan', expect_column_value_to_be_null: 5, expect_column_value_to_be_unique: 9 },
    { date: '4 Jan', expect_column_value_to_be_null: 12, expect_column_value_to_be_unique: 9 },
    { date: '5 Jan', expect_column_value_to_be_null: 15, expect_column_value_to_be_unique: 13 },
    { date: '6 Jan', expect_column_value_to_be_null: 10, expect_column_value_to_be_unique: 6 },
];
const ChartContainer = styled.div`
    padding: 15px 20px 0 20px;
`;
const Title = styled.span`
    font-weight: 600;
    font-size: 12px;
    color: #595959;
`;
const LineChartContainer = styled.div`
    padding: 20px 0;
`;
const DropDown = styled.div`
    text-align: right;
`;
export default function SchemaResultMetricChart() {
    const [chart, setChart] = useState(missingCount);
    const onHandleClick = (value) => {
        switch (value) {
            case 'missingCount':
                setChart(missingCount);
                break;
            case 'elementCount':
                setChart(elementCount);
                break;
            case 'unexpectedCount':
                setChart(unexpectedCount);
                break;
            case 'unexpectedPercent':
                setChart(unexpectedPercent);
                break;
            default:
                setChart(missingCount);
                break;
        }
    };
    return (
        <ChartContainer>
            <Title>Result Metric Chart View:</Title>
            <DropDown>
                <Select defaultValue="missingCount" style={{ width: 120 }} onChange={onHandleClick}>
                    <Option value="missingCount">Missing Count</Option>
                    <Option value="elementCount">Element Count</Option>
                    <Option value="unexpectedCount">Unexpected Count</Option>
                    <Option value="unexpectedPercent">Unexpected Percent</Option>
                </Select>
            </DropDown>
            <LineChartContainer>
                <LineChart
                    width={500}
                    height={300}
                    data={chart}
                    margin={{
                        top: 5,
                        right: 30,
                        left: 20,
                        bottom: 5,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Line
                        type="monotone"
                        dataKey="expect_column_value_to_be_null"
                        stroke="#8884d8"
                        activeDot={{ r: 8 }}
                    />
                    <Line type="monotone" dataKey="expect_column_value_to_be_unique" stroke="#82ca9d" />
                </LineChart>
            </LineChartContainer>
        </ChartContainer>
    );
}
