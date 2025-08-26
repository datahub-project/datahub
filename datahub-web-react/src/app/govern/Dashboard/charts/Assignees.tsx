import { UserOutlined } from '@ant-design/icons';
import { Table } from 'antd';
import React, { useEffect } from 'react';

import { ChartCard } from '@app/dataviz';
import { useFormAnalyticsContext } from '@app/govern/Dashboard/FormAnalyticsContext';
import { ChartNoData, ChartNotEnoughData, ChartState, SectionWaiting } from '@app/govern/Dashboard/charts/AuxViews';
import {
    ChartGroup,
    ChartPerformanceItem,
    ChartPerformanceItems,
    Row,
    SecondaryHeading,
} from '@app/govern/Dashboard/components';
import {
    columnSorterFunction,
    formatPercentage,
    getEntityInfo,
    mergeRowAndHeaderData,
} from '@app/govern/Dashboard/utils';

import { useFormAnalyticsQuery } from '@graphql/analytics.generated';

// March/2024 launch decision: hide performance cards
const hidePerformanceCards = true;

const DocProgressByAssignee = () => {
    const {
        sql,
        sectionLoadStates: { assignees, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.docProgressByAssignee } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !assignees) setLoadStates('assignees', 'progressByAssignee', true);
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
            const { properties, groupInfo } = getEntityInfo(data, row.assignee_urn) || {};
            const displayName = properties ? properties.displayName : groupInfo?.displayName || row.assignee_urn;
            return {
                Name: displayName,
                Total: Number(row.Completed) + Number(row['In Progress']) + Number(row['Not Started']),
                '% Completed': formatPercentage(row.completed_asset_percent),
                Completed: Number(row.Completed),
                'In Progress': Number(row['In Progress']),
                'Not Started': Number(row['Not Started']),
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
            {mergedData.length} total assignees
            <Table
                dataSource={mergedData}
                columns={columns}
                size="small"
                style={{ width: '100%' }}
                pagination={{ pageSize: 10, hideOnSinglePage: true }}
            />
        </div>
    );
};

const DocProgressByAssigneeTopPerforming = () => {
    const {
        sql,
        sectionLoadStates: { assignees, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.assigneeTopPerforming } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !assignees) setLoadStates('assignees', 'assigneeTopPerforming', true);
    }, [loading, setLoadStates, assignees]);

    // States the chart can be in
    const chartState = {
        loading,
        error: !!error,
        noDataTimeframe: !loading && !!data && data?.formAnalytics?.table?.length === 0,
        noData: !data,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []);
    if (mergedData.length === 0) return <ChartNoData />;

    // If there is only 1 record, show a "not enough data" alert (top and least will be the same values)
    if (mergedData.length === 1) return <ChartNotEnoughData />;

    return (
        <ChartPerformanceItems>
            {mergedData.map((d) => {
                const { properties, groupInfo } = getEntityInfo(data, d.assignee_urn) || {};
                const displayName = properties ? properties.displayName : groupInfo?.displayName || d.assignee_urn;
                return (
                    <ChartPerformanceItem key={d.assignee_urn}>
                        <div>
                            <UserOutlined /> {displayName}
                        </div>
                        {formatPercentage(d.completed_asset_percent)} completed
                    </ChartPerformanceItem>
                );
            })}
        </ChartPerformanceItems>
    );
};

const DocProgressByAssigneeLeastPerforming = () => {
    const {
        sql,
        sectionLoadStates: { assignees, setLoadStates },
    } = useFormAnalyticsContext();

    const { data, loading, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.assigneeLeastPerforming } },
        skip: sql.skip,
    });

    useEffect(() => {
        if (!loading && !assignees) setLoadStates('assignees', 'assigneeLeastPerforming', true);
    }, [loading, setLoadStates, assignees]);

    // States the chart can be in
    const chartState = {
        loading,
        error: !!error,
        noDataTimeframe: !!data && data?.formAnalytics?.table?.length === 0,
        noData: !data,
    };

    // Render component to display the chart state
    if (Object.values(chartState).some((v) => v === true)) return <ChartState {...chartState} />;

    const mergedData = mergeRowAndHeaderData(data?.formAnalytics?.header, data?.formAnalytics?.table || []);
    if (mergedData.length === 0) return <ChartNoData />;

    // If there is only 1 record, show a "not enough data" alert (top and least will be the same values)
    if (mergedData.length === 1) return <ChartNotEnoughData />;

    return (
        <ChartPerformanceItems>
            {mergedData.map((d) => {
                const { username, properties } = getEntityInfo(data, d.assignee_urn) || {};
                return (
                    <ChartPerformanceItem key={d.assignee_urn}>
                        <div>
                            <UserOutlined /> {properties?.displayName || username || d.assignee_urn}
                        </div>
                        {formatPercentage(d.completed_asset_percent)} completed
                    </ChartPerformanceItem>
                );
            })}
        </ChartPerformanceItems>
    );
};

export const Assignees = () => {
    const {
        tabs: { selectedTab },
        sectionLoadStates: { forms, questions },
    } = useFormAnalyticsContext();

    // on the forms tab, this section comes after queestions but
    // it's after the forms section otherwise (waterfall render)
    if (selectedTab === 'byForm' && !questions) return <SectionWaiting />;
    if (selectedTab !== 'byForm' && !forms) return <SectionWaiting />;

    return (
        <ChartGroup>
            <SecondaryHeading>Assignees</SecondaryHeading>
            <Row>
                {!hidePerformanceCards && (
                    <Row style={{ flexDirection: 'column', width: 'auto' }}>
                        <ChartCard title="Top Performing Owners" chart={<DocProgressByAssigneeTopPerforming />} />
                        <ChartCard title="Under Performing Owners" chart={<DocProgressByAssigneeLeastPerforming />} />
                    </Row>
                )}
                <ChartCard
                    title="Progress By Assignee"
                    titleInfo="The number of assets this assignee is responsible for and their form status"
                    chart={<DocProgressByAssignee />}
                    flex={2}
                />
            </Row>
        </ChartGroup>
    );
};
