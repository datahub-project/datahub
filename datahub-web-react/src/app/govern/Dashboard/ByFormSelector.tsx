import React from 'react';

import { Select } from 'antd';

import { mergeRowAndHeaderData, getEntityInfo } from './utils';

import { useFormAnalyticsContext } from './FormAnalyticsContext';

export const ByFormSelector = () => {
	const { byForm: { forms, hasForms, setSelectedForm } } = useFormAnalyticsContext();

	console.log('forms', forms);

	// If theres no forms, return null
	if (!hasForms) return null;

	// format the data 
	const data = mergeRowAndHeaderData(forms?.header, forms?.table || []);

	// format options
	const options = data.map((d) => {
		const entity = getEntityInfo(forms, d.form_id);
		return ({
			value: d.form_id,
			label: entity?.info?.name || d.form_id,
		})
	});

	return (
		<Select
			showSearch
			filterOption
			placeholder="Select a form"
			optionFilterProp="label"
			onChange={(value: string) => setSelectedForm(value)}
			options={options}
			defaultValue={options[0].value}
			size="large"
			style={{ width: 300, marginBottom: '2rem' }}
		/>
	);
}	