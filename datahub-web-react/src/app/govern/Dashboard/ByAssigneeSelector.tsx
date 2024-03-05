import React from 'react';

import { Select } from 'antd';

import { mergeRowAndHeaderData, getEntityInfo } from './utils';

import { useFormAnalyticsContext } from './FormAnalyticsContext';

export const ByAssigneeSelector = () => {
	const { byAssignee: { assignees, hasAssignees, setSelectedAssignee } } = useFormAnalyticsContext();

	// If theres no forms, return null
	if (!hasAssignees) return null;

	// format the data 
	const data = mergeRowAndHeaderData(assignees?.header, assignees?.table || []);

	// format options
	const options = data.map((d) => {
		const { username, properties } = getEntityInfo(assignees, d.assignee_urn) || {};
		return ({
			value: d.assignee_urn,
			label: properties.displayName || username || d.assignee_urn,
		});
	});

	return (
		<Select
			showSearch
			filterOption
			placeholder="Select an assignee"
			optionFilterProp="label"
			onChange={(value: string) => setSelectedAssignee(value)}
			options={options}
			defaultValue={options[0].value}
			size="large"
			style={{ width: 300, marginBottom: '2rem' }}
		/>
	);
}	