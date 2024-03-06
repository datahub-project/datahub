import React from 'react';

import { Select } from 'antd';

import { mergeRowAndHeaderData, getEntityInfo } from './utils';

import { useFormAnalyticsContext } from './FormAnalyticsContext';

export const ByAssigneeSelector = () => {
	const {
		byAssignee: { assignees, hasAssignees, setSelectedAssignee },
		sectionLoadStates: { resetLoadStates }
	} = useFormAnalyticsContext();

	// If theres no forms, return null
	if (!hasAssignees) return (
		<Select
			options={[{ value: 'loading', label: 'Loading...' }]}
			size="large"
			style={{ width: 300 }}
			loading
		/>
	);

	// format the data 
	const data = mergeRowAndHeaderData(assignees?.header, assignees?.table || []);

	// format options
	const options = data.map((d) => {
		const { username, properties } = getEntityInfo(assignees, d.assignee_urn) || {};
		return ({
			value: d.assignee_urn,
			label: properties?.displayName || username || d.assignee_urn,
		});
	});

	// Reset load states when form is changed
	const handleSetAssignee = (value: string) => {
		setSelectedAssignee(value)
		resetLoadStates();
	};

	return (
		<Select
			showSearch
			filterOption
			placeholder="Select an assignee"
			optionFilterProp="label"
			onChange={handleSetAssignee}
			options={options}
			defaultValue={options[0].value}
			size="large"
			style={{ width: 300 }}
		/>
	);
}	