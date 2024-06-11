import React, { useState } from 'react';

import { uniq, orderBy } from 'lodash';

import { useListTestsQuery } from '../../../graphql/test.generated';
import { useListActionPipelinesQuery } from '../../../graphql/actionPipeline.generated';

import {
	AutomationsPageContainer,
	AutomationsSidebar,
	AutomationsContent,
	AutomationsContentHeader,
	AutomationsContentBody,
	AutomationsContentTabs,
	AutomationsContentTab,
	AutomationsBody,
} from './components';

import { LargeButtonPrimary } from '../sharedComponents';

import { env } from '../constants';
import { simplifyDataForListView } from '../utils';

import { AutomationsListCard } from './ListCard';
import { AutomationModal } from './Modal';

export const Automations = () => {
	// Rollout Variables (UI only)
	const { hideSidebar, hideMetadataTests } = env;

	// Create Modal State
	const [isOpen, setIsOpen] = useState(false);

	// Fetch metadata tests
	const { data: metadataTestsData } = useListTestsQuery({
		variables: {
			input: {
				start: 0,
				count: 10,
			},
		},
		skip: hideMetadataTests
	});

	// Fetch action pipelines
	const { data: actionPipelinesData } = useListActionPipelinesQuery({
		variables: {
			input: {
				start: 0,
				count: 10,
			},
		},
	});

	// Raw Data
	const metadataTests = metadataTestsData?.listTests?.tests || [];
	const actionPipelines = actionPipelinesData?.listActionPipelines?.actionPipelines || [];

	// Simplify Data for List View
	const simplifiedMetadataTests = simplifyDataForListView(metadataTests);
	const simplifiedActionPipelines = simplifyDataForListView(actionPipelines);

	// All Automations
	const allAutomations = [...simplifiedActionPipelines, ...simplifiedMetadataTests];

	// Get Categories
	const categories = uniq(allAutomations.map((automation: any) => automation.category));

	// Build tabs
	const tabs: any = [
		{
			key: 'all',
			label: 'All',
			data: allAutomations,
			count: allAutomations.length,
		},
	];

	categories.forEach((category: string) => {
		tabs.push({
			key: category,
			label: category,
			data: allAutomations.filter((automation: any) => automation.category === category),
			count: allAutomations.filter((automation: any) => automation.category === category).length,
		});
	});

	const [activeTab, setActiveTab] = useState(tabs[0].key);
	const data = tabs.filter((tab) => tab.key === activeTab)[0].data || [];

	// Data states
	// const isLoading = testsLoading || actionsLoading;
	// const isError = testsError || actionsError;
	// const noData = allAutomations.length === 0;

	return (
		<>
			<AutomationsPageContainer>
				{!hideSidebar && (
					<AutomationsSidebar>
						<h1>Sidebar</h1>
					</AutomationsSidebar>
				)}
				<AutomationsContent>
					<AutomationsContentHeader>
						<div>
							<h1>Automations</h1>
							<p>Monitor policies and automate actions across data assets.</p>
						</div>
						<div>
							<LargeButtonPrimary onClick={() => setIsOpen(!isOpen)}>
								Create an Automation
							</LargeButtonPrimary>
						</div>
					</AutomationsContentHeader>
					<AutomationsContentBody>
						<AutomationsContentTabs>
							{orderBy(tabs, ['count'], ['desc']).map((tab) => (
								<AutomationsContentTab
									key={tab.key}
									isActive={activeTab === tab.key}
									onClick={() => setActiveTab(tab.key)}
								>
									{tab.label}
									<span>{tab.count}</span>
								</AutomationsContentTab>
							))}
						</AutomationsContentTabs>
						<AutomationsBody>
							{data.map((item) => <AutomationsListCard key={item.key} automation={item} />)}
						</AutomationsBody>
					</AutomationsContentBody>
				</AutomationsContent>
			</AutomationsPageContainer>
			<AutomationModal isOpen={isOpen} setIsOpen={setIsOpen} />
		</>
	);
};
