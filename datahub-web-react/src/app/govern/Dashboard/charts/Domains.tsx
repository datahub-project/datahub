import React, { useEffect } from 'react';

import { Table } from 'antd';
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

import DomainIcon from '../../../domain/DomainIcon';

import { SectionWaiting, ChartState, ChartNoData, ChartNotEnoughData } from './AuxViews';

// March/2024 launch decision: hide performance cards
const hidePerformanceCards = true;

const DocumentationProgressPerDomain = () => {
    const {
        sql,
        sectionLoadStates: { domains, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.docProgressByDomain } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !domains) setLoadStates('domains', 'progressByDomain', true);
    }, [loading, setLoadStates, domains]);

    // States the chart can be in
    const chartState = {
        loading,
        error: !!error,
        noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
        noData: data?.formAnalytics?.table?.length === 0,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []).map((d) => {
        return {
            ...d,
            domain_urn:
                truncateString(getEntityInfo(data, d.domain_urn)?.properties?.name) || d.domain_urn || 'No Domain',
        };
    });
    if (mergedData.length === 0) return <ChartNoData />;

    const datakeys = Object.keys(mergedData[0]).filter((k) => k !== 'domain_urn');

    return (
        <HorizontalBarChart
            data={mergedData}
            dataKeys={datakeys}
            yAccessor={(d: { domain_urn: string }) => d.domain_urn}
            colorAccessor={statusOrdinalScale}
        />
    );
};

const CompletionPerformanceByDomain = () => {
    const {
        sql,
        sectionLoadStates: { assignees, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.completionPerformanceByDomain } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !assignees) setLoadStates('domains', 'domainTopPerforming', true);
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
            // const { properties: { name } } = getEntityInfo(data, row.domain) || {};
            const name = getEntityInfo(data, row.domain_urn)?.properties?.name || row.domain || 'No Domain';
            return {
                Domain: name,
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

const DocProgressByDomainTopPerforming = () => {
    const {
        sql,
        sectionLoadStates: { domains, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.domainTopPerforming } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !domains) setLoadStates('domains', 'domainTopPerforming', true);
    }, [loading, setLoadStates, domains]);

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
                <ChartPerformanceItem key={d.domain_urn}>
                    <div>
                        <DomainIcon /> {getEntityInfo(data, d.domain_urn)?.properties?.name || d.domain_urn}
                    </div>
                    {formatPercentage(d.completed_asset_percent)} completed
                </ChartPerformanceItem>
            ))}
        </ChartPerformanceItems>
    );
};

const DocProgressByDomainLeastPerforming = () => {
    const {
        sql,
        sectionLoadStates: { domains, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.domainLeastPerforming } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !domains) setLoadStates('domains', 'domainLeastPerforming', true);
    }, [loading, setLoadStates, domains]);

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
                <ChartPerformanceItem key={d.domain_urn}>
                    <div>
                        <DomainIcon /> {getEntityInfo(data, d.domain_urn)?.properties?.name || d.domain_urn}
                    </div>
                    {formatPercentage(d.completed_asset_percent)} completed
                </ChartPerformanceItem>
            ))}
        </ChartPerformanceItems>
    );
};

export const Domains = () => {
    const {
        tabs: { selectedTab },
        sectionLoadStates: { forms, assignees },
    } = useFormAnalyticsContext();

    // on the assingee tab, this section comes after forms but
    // it's after the assignee section otherwise (waterfall render)
    if (selectedTab === 'byAssignee' && !forms) return <SectionWaiting />;
    if (selectedTab !== 'byAssignee' && !assignees) return <SectionWaiting />;

    return (
        <ChartGroup>
            <SecondaryHeading>Domains</SecondaryHeading>
            <Row>
                <ChartCard
                    title="Progress By Domain"
                    titleInfo="Shows form status for the number of assets in their domain"
                    chart={<DocumentationProgressPerDomain />}
                    flex={2}
                />
                {!hidePerformanceCards && (
                    <Row style={{ flexDirection: 'column', width: 'auto' }}>
                        <ChartCard title="Top Performing Domains" chart={<DocProgressByDomainTopPerforming />} />
                        <ChartCard title="Under Performing Domains" chart={<DocProgressByDomainLeastPerforming />} />
                    </Row>
                )}
                <Row style={{ flexDirection: 'column', width: 'auto' }}>
                    <ChartCard title="Completion Performance by Domain" chart={<CompletionPerformanceByDomain />} />
                </Row>
            </Row>
        </ChartGroup>
    );
};
