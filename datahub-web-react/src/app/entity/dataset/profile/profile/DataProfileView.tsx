import { Divider, Row, Space, Table, Tag, Typography } from 'antd';
import { ColumnsType } from 'antd/lib/table';
import React, { useMemo } from 'react';

import { DatasetProfile } from '../../../../../types.generated';
import { Highlight } from '../../../../analyticsDashboard/components/Highlight';

export type Props = {
    profile: DatasetProfile;
};

export default function DataProfileView({ profile }: Props) {
    const tableData = useMemo(
        () =>
            profile?.fieldProfiles?.map((doc) => ({
                name: doc.fieldPath,
                min: doc.min,
                max: doc.max,
                mean: doc.mean,
                median: doc.median,
                nullCount: doc.nullCount && doc.nullCount.toString(),
                nullPercentage: doc.nullProportion ? `${(doc.nullProportion * 100).toFixed()}%` : undefined,
                distinctCount: (doc.uniqueCount && doc.uniqueCount.toString()) || undefined,
                distinctPercentage: doc.uniqueProportion ? `${(doc.uniqueProportion * 100).toFixed()}%` : undefined,
                sampleValues: doc.sampleValues || [],
            })),
        [profile],
    );

    const unknownValue = () => {
        return <Typography.Text style={{ color: '#B8B8B8' }}>unknown</Typography.Text>;
    };

    const anyItemContainsKey = (arrayObj: Array<any>, fieldName: string) => {
        return arrayObj.some((value) => fieldName in value && value[fieldName]);
    };

    const buildTableColumns = (data) => {
        // Dynamically builds table columns based on the column stats present in the profile.

        const columns: ColumnsType<any> = [
            {
                title: 'Name',
                dataIndex: 'name',
            },
        ];

        if (anyItemContainsKey(data, 'min')) {
            columns.push({
                title: 'Min',
                dataIndex: 'min',
                render: (value) => value || unknownValue(),
            });
        }

        if (anyItemContainsKey(data, 'max')) {
            columns.push({
                title: 'Max',
                dataIndex: 'max',
                render: (value) => value || unknownValue(),
            });
        }

        if (anyItemContainsKey(data, 'mean')) {
            columns.push({
                title: 'Mean',
                dataIndex: 'mean',
                render: (value) => value || unknownValue(),
            });
        }

        if (anyItemContainsKey(data, 'median')) {
            columns.push({
                title: 'Median',
                dataIndex: 'median',
                render: (value) => value || unknownValue(),
            });
        }

        if (anyItemContainsKey(data, 'nullCount')) {
            columns.push({
                title: 'Null Count',
                dataIndex: 'nullCount',
                render: (value) => value || unknownValue(),
            });
        }

        if (anyItemContainsKey(data, 'nullPercentage')) {
            columns.push({
                title: 'Null %',
                dataIndex: 'nullPercentage',
                render: (value) => value || unknownValue(),
            });
        }

        if (anyItemContainsKey(data, 'distinctCount')) {
            columns.push({
                title: 'Distinct Count',
                dataIndex: 'distinctCount',
                render: (value) => value || unknownValue(),
            });
        }

        if (anyItemContainsKey(data, 'distinctPercentage')) {
            columns.push({
                title: 'Distinct %',
                dataIndex: 'distinctPercentage',
                render: (value) => value || unknownValue(),
            });
        }

        if (anyItemContainsKey(data, 'sampleValues')) {
            columns.push({
                title: 'Sample Values',
                dataIndex: 'sampleValues',
                render: (sampleValues: Array<string>) => {
                    return (
                        (sampleValues &&
                            sampleValues
                                .slice(0, sampleValues.length < 3 ? sampleValues?.length - 1 : 2)
                                .map((value) => <Tag>{value}</Tag>)) ||
                        unknownValue()
                    );
                },
            });
        }

        if (anyItemContainsKey(data, 'quantiles') || anyItemContainsKey(data, 'distinctValueFrequencies')) {
            columns.push({
                title: '',
                render: () => 'see more',
            });
        }

        return columns;
    };

    const tableColumns = buildTableColumns(tableData);

    return (
        <Space direction="vertical" size={60} style={{ width: '100%' }}>
            <Space direction="vertical" style={{ width: '100%' }}>
                <Typography.Title level={3}>Table Stats</Typography.Title>
                <Divider style={{ margin: 0 }} />
                <Row align="top" justify="start">
                    <Highlight highlight={{ value: profile?.rowCount || 0, title: 'Rows', body: '' }} />
                    <Highlight highlight={{ value: profile?.columnCount || 0, title: 'Columns', body: '' }} />
                </Row>
            </Space>
            <Space direction="vertical" style={{ marginBottom: 80, width: '80%' }}>
                <Typography.Title level={3}>Column Stats</Typography.Title>
                <Divider style={{ margin: 0 }} />
                <Table
                    bordered
                    style={{ marginTop: 15 }}
                    pagination={false}
                    columns={tableColumns}
                    dataSource={tableData}
                />
            </Space>
        </Space>
    );
}
