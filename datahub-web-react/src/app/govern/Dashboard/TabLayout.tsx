import React, { useState, useEffect } from 'react';

import { Tabs, Tooltip } from 'antd';
import { json2csv } from 'json-2-csv';
import dayjs from 'dayjs';
import { DownloadOutlined } from '@ant-design/icons';

import { SeriesSelect } from './SeriesSelect';
import { Assignees, Domains, Forms, Stats, OverallProgress, Questions } from './charts';

import { ByFormSelector } from './ByFormSelector';
import { ByAssigneeSelector } from './ByAssigneeSelector';
import { ByDomainSelector } from './ByDomainSelector';

import { mergeRowAndHeaderData } from './utils';

import { useFormAnalyticsContext } from './FormAnalyticsContext';
import { useFormAnalyticsQuery } from '../../../graphql/analytics.generated';

import {
	Layout,
	Header,
	TabsContainer,
	Body,
	PrimaryHeading,
	BodyHeader
} from './components';

interface Tab {
	key: string;
	label: string;
	disabled?: boolean;
	charts: Array<React.ReactElement>,
}

export const TabLayout = () => {
	const {
		sql,
		contextLoading,
		snapshot,
		tabs: { selectedTab, setSelectedTab },
		byForm: { hasForms },
		byAssignee: { hasAssignees },
		byDomain: { hasDomains },
	} = useFormAnalyticsContext();

	const [isDownloadingCSV, setIsDownloadingCSV] = useState(false);

	// Define the tabs
	const tabs: Tab[] = [
		{
			key: 'overall',
			label: 'Overall',
			charts: [
				<OverallProgress />,
				<Forms />,
				<Assignees />,
				<Domains />,
			]
		},
		{
			key: 'byForm',
			label: 'By Form',
			disabled: !hasForms,
			charts: [
				<Stats />,
				<OverallProgress />,
				<Questions />,
				<Assignees />,
				<Domains />,
			],
		},
		{
			key: 'byAssignee',
			label: 'By Assignee',
			disabled: !hasAssignees,
			charts: [
				<Stats />,
				<OverallProgress />,
				<Forms />,
				<Domains />,
			],
		},
		{
			key: 'byDomain',
			label: 'By Domain',
			disabled: !hasDomains,
			charts: [
				<Stats />,
				<OverallProgress />,
				<Forms />,
				<Assignees />,
			],
		}
	];

	// Handle changing the tab
	const handleSetTab = (t: any) => setSelectedTab(t);

	// Get charts for selected tab
	const thisTab = tabs.find((t) => t.key === selectedTab) as Tab;
	const charts = thisTab?.charts;

	// Loading state for data hydration
	const showLoadingState = contextLoading;

	// Fetch CSV JSON when we user triggers state change
	const { data: csvData } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': sql.downloadCSVJSON } },
		skip: !snapshot
	});

	// Handle download CSV
	const handleDownloadCSV = (e) => {
		e.preventDefault();
		e.stopPropagation();
		setIsDownloadingCSV(true);
	}

	// If we have the data, download the CSV
	useEffect(() => {
		if (isDownloadingCSV && csvData) {
			const mergedData = mergeRowAndHeaderData(csvData?.formAnalytics?.header, csvData?.formAnalytics?.table || []);
			const csv = json2csv(mergedData);
			const blob = new Blob([csv], { type: 'text/csv' });
			const url = window.URL.createObjectURL(blob);
			const a = document.createElement('a');
			const timestamp = dayjs().format('YYYYMMDD');
			a.href = url;
			a.download = `documentation-metrics-${timestamp}.csv`;
			a.click();
			window.URL.revokeObjectURL(url);
		}
		setIsDownloadingCSV(false);
	}, [csvData, isDownloadingCSV, setIsDownloadingCSV]);

	return (
		<Layout>
			<Header>
				<PrimaryHeading>Your Documentation Initiatives</PrimaryHeading>
				{/* <Button type="primary" onClick={handleDownloadCSV}>Download CSV</Button> */}
			</Header>
			<TabsContainer>
				<Tabs defaultActiveKey={selectedTab} items={tabs} onChange={handleSetTab} />
				<SeriesSelect />
			</TabsContainer>
			<BodyHeader>
				<Tooltip title="Download Results" placement="left">
					<DownloadOutlined style={{ marginRight: '0px', fontSize: '20px' }} onClick={handleDownloadCSV} />
				</Tooltip>
			</BodyHeader>
			<Body>
				{/* <Button type="primary" onClick={handleDownloadCSV}>Download CSV</Button> */}
				{/* </div> */}
				{showLoadingState && 'Loading...'}
				{!showLoadingState &&
					<>
						{thisTab?.key === 'byForm' && !thisTab?.disabled && <ByFormSelector />}
						{thisTab?.key === 'byAssignee' && !thisTab?.disabled && <ByAssigneeSelector />}
						{thisTab?.key === 'byDomain' && !thisTab?.disabled && <ByDomainSelector />}
						{!thisTab?.disabled ? charts.map((chart) => chart) : 'No data for this tab during this timeframe.'}
					</>
				}
			</Body>
		</Layout>
	);
};
