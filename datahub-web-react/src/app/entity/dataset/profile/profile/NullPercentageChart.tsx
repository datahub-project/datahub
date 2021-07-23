import { Col, Divider, Typography } from 'antd';
import React from 'react';
import { DatasetProfile, DateInterval, DateRange, NumericDataPoint } from '../../../../../types.generated';
import { ChartCard } from '../../../../analyticsDashboard/components/ChartCard';
import { ChartContainer } from '../../../../analyticsDashboard/components/ChartContainer';
import { TimeSeriesChart } from '../../../../analyticsDashboard/components/TimeSeriesChart';

export type Props = {
    profiles: Array<DatasetProfile>;
    fieldPath: string;
    interval: DateInterval;
    dateRange: DateRange;
};

export default function NullPercentageChart({ profiles, fieldPath, interval, dateRange }: Props) {
    const data = profiles
        .map((profile) => {
            const dateStr = new Date(profile.timestampMillis).toISOString();

            const fieldProfiles = profile.fieldProfiles
                ?.filter((field) => field.fieldPath === fieldPath)
                .filter((field) => field.nullProportion !== null && field.nullProportion !== undefined);

            if (fieldProfiles?.length === 1) {
                const fieldProfile = fieldProfiles[0];
                return {
                    x: dateStr,
                    y: fieldProfile.nullProportion as number,
                };
            }
            return null;
        })
        .filter((dataPoint) => dataPoint !== null)
        .map((dataPoint) => dataPoint as NumericDataPoint)
        .reverse();

    const chartData = {
        title: 'Null Percentage Over Time',
        lines: [
            {
                name: 'Null Percentage',
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
                            style={{ lineColor: '#20d3bd', axisColor: '#D8D8D8', axisWidth: 2 }}
                            hideLegend
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
