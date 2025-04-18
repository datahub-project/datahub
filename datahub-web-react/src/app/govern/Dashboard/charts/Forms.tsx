import React, { useEffect } from 'react';

import { Table } from 'antd';
import { FormOutlined } from '@ant-design/icons';

import { ChartCard, HorizontalBarChart } from '../../../dataviz';
import { ChartGroup, Row, SecondaryHeading, ChartPerformanceItems, ChartPerformanceItem } from '../components';

import {
    statusOrdinalScale,
    mergeRowAndHeaderData,
    formatPercentage,
    getEntityInfo,
    truncateString,
    columnSorterFunction,
} from '../utils';
import { useFormAnalyticsQuery } from '../../../../graphql/analytics.generated';
import { useFormAnalyticsContext } from '../FormAnalyticsContext';

import { SectionWaiting, ChartState, ChartNoData, ChartNotEnoughData } from './AuxViews';

// March/2024 launch decision: hide performance cards
const hidePerformanceCards = true;

const DocumentationProgressPerForm = () => {
    const {
        sql,
        sectionLoadStates: { forms, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.docProgressByForm } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !forms) setLoadStates('forms', 'progressByForm', true);
    }, [loading, setLoadStates, forms]);

    // States the chart can be in
    const chartState = {
        loading,
        error: !!error,
        noDataTimeframe: data?.formAnalytics?.table?.length === 0,
        noData: data?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []).map((d) => {
        return {
            ...d,
            form: truncateString(getEntityInfo(data, d.form)?.info?.name) || d.form,
        };
    });
    if (mergedData.length === 0) return <ChartNoData />;

    const datakeys = Object.keys(mergedData[0]).filter((k) => k !== 'form');

    return (
        <HorizontalBarChart
            data={mergedData}
            dataKeys={datakeys}
            yAccessor={(d: { form: string }) => d.form}
            colorAccessor={statusOrdinalScale}
        />
    );
};

const CompletionPerformanceByForm = () => {
    const {
        sql,
        sectionLoadStates: { assignees, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.completionPerformanceByForm } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !assignees) setLoadStates('forms', 'formTopPerforming', true);
    }, [loading, setLoadStates, assignees]);

    // Update the chart states
    const chartState = {
        loading,
        error: !!error,
        noDataTimeframe: !loading && !!data && data?.formAnalytics?.table?.length === 0,
        noData: data?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []).map(
        (row) => {
            const name = getEntityInfo(data, row.form)?.info?.name || row.form;
            return {
                Form: name,
                Total: Number(row.Completed) + Number(row['In Progress']) + Number(row['Not Started']),
                '% Completed': formatPercentage(row.completed_asset_percent),
            };
        },
    );

    if (mergedData.length === 0) return <ChartNoData />;

    const columns = Object.keys(mergedData[0]).map((key) => ({
        key,
        dataIndex: key,
        title: key,
        sorter: (a, b) => columnSorterFunction(a, b, key),
    }));

    return (
        <div style={{ width: '100%', marginTop: '1rem' }}>
            <Table
                dataSource={mergedData}
                columns={columns}
                size="small"
                style={{ width: '100%' }}
                pagination={{ pageSize: 5, hideOnSinglePage: true }}
            />
        </div>
    );
};

const DocumentationProgressTopPerforming = () => {
    const {
        sql,
        sectionLoadStates: { forms, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.formTopPerforming } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !forms) setLoadStates('forms', 'formTopPerforming', true);
    }, [loading, setLoadStates, forms]);

    // States the chart can be in
    const chartState = {
        loading,
        error: !!error,
        noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
        noData: data?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []);
    if (mergedData.length === 0) return <ChartNoData />;

    // If there is only 1 record, show a "not enough data" alert (top and least will be the same values)
    if (mergedData.length === 1) return <ChartNotEnoughData />;

    return (
        <ChartPerformanceItems>
            {mergedData.map((d) => (
                <ChartPerformanceItem key={d.form_urn}>
                    <div>
                        <FormOutlined /> {getEntityInfo(data, d.form_urn)?.info?.name || d.form}
                    </div>
                    {formatPercentage(d.completed_asset_percent)} completed
                </ChartPerformanceItem>
            ))}
        </ChartPerformanceItems>
    );
};

const DocumentationProgressLeastPerforming = () => {
    const {
        sql,
        sectionLoadStates: { forms, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.formLeastPerforming } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !forms) setLoadStates('forms', 'formLeastPerforming', true);
    }, [loading, setLoadStates, forms]);

    // States the chart can be in
    const chartState = {
        loading,
        error: !!error,
        noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
        noData: data?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []);
    if (mergedData.length === 0) return <ChartNoData />;

    // If there is only 1 record, show a "not enough data" alert (top and least will be the same values)
    if (mergedData.length === 1) return <ChartNotEnoughData />;

    return (
        <ChartPerformanceItems>
            {mergedData.map((d) => (
                <ChartPerformanceItem key={d.form}>
                    <div>
                        <FormOutlined /> {getEntityInfo(data, d.form)?.info?.name || d.form}
                    </div>
                    {formatPercentage(d.completed_asset_percent)} completed
                </ChartPerformanceItem>
            ))}
        </ChartPerformanceItems>
    );
};

export const Forms = () => {
    const {
        sectionLoadStates: { overallProgress },
    } = useFormAnalyticsContext();

    // This section currently always comes after the overall progress section (waterfall render)
    if (!overallProgress) return <SectionWaiting />;

    return (
        <ChartGroup>
            <SecondaryHeading>Forms</SecondaryHeading>
            <Row>
                <ChartCard title="Documentation Progress by Form" chart={<DocumentationProgressPerForm />} flex={2} />
                {!hidePerformanceCards && (
                    <Row style={{ flexDirection: 'column', width: 'auto' }}>
                        <ChartCard title="Top Performing Forms" chart={<DocumentationProgressTopPerforming />} />
                        <ChartCard title="Under Performing Forms" chart={<DocumentationProgressLeastPerforming />} />
                    </Row>
                )}
                <Row style={{ flexDirection: 'column', width: 'auto' }}>
                    <ChartCard title="Completion Performance by Form" chart={<CompletionPerformanceByForm />} />
                </Row>
            </Row>
        </ChartGroup>
    );
};
