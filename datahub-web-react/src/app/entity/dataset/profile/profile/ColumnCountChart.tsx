import { Col, Divider, Typography } from 'antd';
import React from 'react';
import { DatasetProfile, DateInterval, DateRange } from '../../../../../types.generated';
import { ChartCard } from '../../../../analyticsDashboard/components/ChartCard';
import { ChartContainer } from '../../../../analyticsDashboard/components/ChartContainer';
import { TimeSeriesChart } from '../../../../analyticsDashboard/components/TimeSeriesChart';

export type Props = {
    profiles: Array<DatasetProfile>;
    interval: DateInterval;
    dateRange: DateRange;
};

export default function ColumnCountChart({ profiles, interval, dateRange }: Props) {
    const data = profiles
        .filter((profile) => profile.rowCount)
        .map((profile) => {
            const dateStr = new Date(profile.timestampMillis).toISOString();
            return {
                x: dateStr,
                y: profile.columnCount as number,
            };
        })
        .reverse();

    const chartData = {
        title: 'Column Count Over Time',
        lines: [
            {
                name: 'Column Count',
                data,
            },
        ],
        interval,
        dateRange,
    };

    return (
        <>
            <Col sm={24} md={24} lg={8} xl={8}>
                <ChartCard shouldScroll={false}>
                    <ChartContainer>
                        <div style={{ width: '100%', marginBottom: 20 }}>
                            <Typography.Title level={5}>{chartData.title}</Typography.Title>
                        </div>
                        <Divider style={{ margin: 0, padding: 0 }} />
                        <TimeSeriesChart
                            hideLegend
                            style={{ lineColor: '#20d3bd', axisColor: '#D8D8D8', axisWidth: 2 }}
                            chartData={chartData}
                            width={360}
                            height={300}
                        />
                    </ChartContainer>
                </ChartCard>
            </Col>
        </>
    );
}
