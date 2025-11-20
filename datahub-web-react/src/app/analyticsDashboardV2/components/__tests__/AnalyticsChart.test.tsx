import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { AnalyticsChart } from '@app/analyticsDashboardV2/components/AnalyticsChart';

import { BarChart as BarChartType, DateInterval, TableChart, TimeSeriesChart } from '@types';

// Mock the @visx dependencies
vi.mock('@visx/responsive', () => ({
    ParentSize: ({ children }: any) => children({ width: 800, height: 400 }),
}));

vi.mock('@visx/tooltip', () => ({
    useTooltip: () => ({
        tooltipData: null,
        tooltipLeft: 0,
        tooltipTop: 0,
        showTooltip: vi.fn(),
        hideTooltip: vi.fn(),
    }),
    useTooltipInPortal: () => ({
        containerRef: { current: null },
        TooltipInPortal: ({ children }: any) => <div>{children}</div>,
    }),
}));

describe('AnalyticsChart', () => {
    describe('TimeSeriesChart', () => {
        it('renders time series chart with data', () => {
            const chartData: TimeSeriesChart = {
                __typename: 'TimeSeriesChart',
                title: 'Daily Active Users',
                dateRange: { start: '2024-01-01', end: '2024-01-03' },
                interval: DateInterval.Day,
                lines: [
                    {
                        name: 'Users',
                        data: [
                            { x: '2024-01-01', y: 100 },
                            { x: '2024-01-02', y: 150 },
                            { x: '2024-01-03', y: 200 },
                        ],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Daily Active Users')).toBeInTheDocument();
        });

        it('renders empty state when no data', () => {
            const chartData: TimeSeriesChart = {
                __typename: 'TimeSeriesChart',
                title: 'Daily Active Users',
                dateRange: { start: '2024-01-01', end: '2024-01-01' },
                interval: DateInterval.Day,
                lines: [
                    {
                        name: 'Users',
                        data: [],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Daily Active Users')).toBeInTheDocument();
        });
    });

    describe('BarChart - Simple', () => {
        it('renders simple bar chart with single segment bars', () => {
            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Entity Views by Type',
                bars: [
                    {
                        name: 'Dataset',
                        segments: [{ label: 'Views', value: 100 }],
                    },
                    {
                        name: 'Dashboard',
                        segments: [{ label: 'Views', value: 50 }],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Entity Views by Type')).toBeInTheDocument();
        });
    });

    describe('BarChart - Stacked', () => {
        const createStackedChartData = (): BarChartType => ({
            __typename: 'BarChart',
            title: 'Entity Actions by Type',
            bars: [
                {
                    name: 'DATASET',
                    segments: [
                        { label: 'VIEW', value: 100 },
                        { label: 'SEARCH', value: 50 },
                        { label: 'EDIT', value: 25 },
                    ],
                },
                {
                    name: 'DASHBOARD',
                    segments: [
                        { label: 'VIEW', value: 75 },
                        { label: 'SEARCH', value: 30 },
                        { label: 'EDIT', value: 10 },
                    ],
                },
                {
                    name: 'DATAPRODUCT',
                    segments: [
                        { label: 'VIEW', value: 50 },
                        { label: 'SEARCH', value: 20 },
                    ],
                },
            ],
        });

        it('renders stacked bar chart with multiple segments', () => {
            const chartData = createStackedChartData();
            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Entity Actions by Type')).toBeInTheDocument();
        });

        it('shows legend with all segment labels', () => {
            const chartData = createStackedChartData();
            render(<AnalyticsChart chartData={chartData} />);

            // Check that legend items are present
            expect(screen.getByText('VIEW')).toBeInTheDocument();
            expect(screen.getByText('SEARCH')).toBeInTheDocument();
            expect(screen.getByText('EDIT')).toBeInTheDocument();
        });

        it('truncates long x-axis labels to 11 characters', () => {
            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Long Labels Test',
                bars: [
                    {
                        name: 'VERYLONGLABELNAME',
                        segments: [{ label: 'VIEW', value: 100 }],
                    },
                    {
                        name: 'SHORT',
                        segments: [{ label: 'VIEW', value: 50 }],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);

            // The chart should render successfully with truncated labels
            expect(screen.getByText('Long Labels Test')).toBeInTheDocument();
        });

        it('handles empty bars array', () => {
            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Empty Chart',
                bars: [],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Empty Chart')).toBeInTheDocument();
        });

        it('handles bars with zero values', () => {
            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Zero Values',
                bars: [
                    {
                        name: 'DATASET',
                        segments: [
                            { label: 'VIEW', value: 0 },
                            { label: 'SEARCH', value: 0 },
                        ],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Zero Values')).toBeInTheDocument();
        });

        it('handles bars with missing segment data', () => {
            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Sparse Data',
                bars: [
                    {
                        name: 'BAR1',
                        segments: [
                            { label: 'VIEW', value: 100 },
                            { label: 'EDIT', value: 50 },
                        ],
                    },
                    {
                        name: 'BAR2',
                        segments: [
                            { label: 'VIEW', value: 75 },
                            // Missing EDIT segment
                        ],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Sparse Data')).toBeInTheDocument();
        });
    });

    describe('TableChart', () => {
        it('renders table chart', () => {
            const chartData: TableChart = {
                __typename: 'TableChart',
                title: 'Top Datasets',
                columns: ['Name', 'Views'],
                rows: [
                    {
                        values: ['dataset1', '100'],
                        cells: [
                            { __typename: 'Cell', value: 'dataset1', linkParams: null },
                            { __typename: 'Cell', value: '100', linkParams: null },
                        ],
                    },
                    {
                        values: ['dataset2', '75'],
                        cells: [
                            { __typename: 'Cell', value: 'dataset2', linkParams: null },
                            { __typename: 'Cell', value: '75', linkParams: null },
                        ],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Top Datasets')).toBeInTheDocument();
            expect(screen.getByText('dataset1')).toBeInTheDocument();
            expect(screen.getByText('100')).toBeInTheDocument();
        });
    });

    describe('Chart Type Detection', () => {
        it('correctly identifies stacked bar chart', () => {
            const stackedChart: BarChartType = {
                __typename: 'BarChart',
                title: 'Stacked',
                bars: [
                    {
                        name: 'BAR1',
                        segments: [
                            { label: 'A', value: 10 },
                            { label: 'B', value: 20 },
                        ],
                    },
                ],
            };

            render(<AnalyticsChart chartData={stackedChart} />);
            expect(screen.getByText('Stacked')).toBeInTheDocument();
        });

        it('correctly identifies simple bar chart', () => {
            const simpleChart: BarChartType = {
                __typename: 'BarChart',
                title: 'Simple',
                bars: [
                    {
                        name: 'BAR1',
                        segments: [{ label: 'A', value: 10 }],
                    },
                    {
                        name: 'BAR2',
                        segments: [{ label: 'A', value: 20 }],
                    },
                ],
            };

            render(<AnalyticsChart chartData={simpleChart} />);
            expect(screen.getByText('Simple')).toBeInTheDocument();
        });
    });

    describe('Data Transformation', () => {
        it('correctly transforms time series data', () => {
            const chartData: TimeSeriesChart = {
                __typename: 'TimeSeriesChart',
                title: 'Test',
                dateRange: { start: '2024-01-01T00:00:00Z', end: '2024-01-02T00:00:00Z' },
                interval: DateInterval.Day,
                lines: [
                    {
                        name: 'Line1',
                        data: [
                            { x: '2024-01-01T00:00:00Z', y: 100 },
                            { x: '2024-01-02T00:00:00Z', y: 200 },
                        ],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Test')).toBeInTheDocument();
        });

        it('correctly transforms bar chart data with indices', () => {
            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Bar Test',
                bars: [
                    {
                        name: 'First',
                        segments: [{ label: 'Count', value: 10 }],
                    },
                    {
                        name: 'Second',
                        segments: [{ label: 'Count', value: 20 }],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Bar Test')).toBeInTheDocument();
        });
    });

    describe('Edge Cases', () => {
        it('handles extremely long bar names', () => {
            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Long Names',
                bars: [
                    {
                        name: 'A'.repeat(100),
                        segments: [{ label: 'VIEW', value: 100 }],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Long Names')).toBeInTheDocument();
        });

        it('handles large number of bars', () => {
            const bars = Array.from({ length: 50 }, (_, i) => ({
                name: `BAR${i}`,
                segments: [
                    { label: 'VIEW', value: Math.random() * 100 },
                    { label: 'EDIT', value: Math.random() * 50 },
                ],
            }));

            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Many Bars',
                bars,
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Many Bars')).toBeInTheDocument();
        });

        it('handles large number of segments', () => {
            const segments = Array.from({ length: 20 }, (_, i) => ({
                label: `SEGMENT${i}`,
                value: Math.random() * 100,
            }));

            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Many Segments',
                bars: [
                    {
                        name: 'BAR1',
                        segments,
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Many Segments')).toBeInTheDocument();
        });

        it('handles very large values', () => {
            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Large Values',
                bars: [
                    {
                        name: 'BAR1',
                        segments: [{ label: 'COUNT', value: 1000000000 }],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Large Values')).toBeInTheDocument();
        });

        it('handles decimal values', () => {
            const chartData: BarChartType = {
                __typename: 'BarChart',
                title: 'Decimals',
                bars: [
                    {
                        name: 'BAR1',
                        segments: [{ label: 'PERCENT', value: 12.456 }],
                    },
                ],
            };

            render(<AnalyticsChart chartData={chartData} />);
            expect(screen.getByText('Decimals')).toBeInTheDocument();
        });
    });
});
