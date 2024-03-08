import React from 'react';

import { Select } from 'antd';

import { mergeRowAndHeaderData, getEntityInfo } from './utils';

import { useFormAnalyticsContext } from './FormAnalyticsContext';
import { StyledSelect } from './components';

export const ByFormSelector = () => {
	const {
		byForm: { forms, hasForms, setSelectedForm },
		sectionLoadStates: { resetLoadStates }
	} = useFormAnalyticsContext();

	// If theres no forms, return null
	if (!hasForms) return (
		<Select
			options={[{ value: 'loading', label: 'Loading...' }]}
			size="large"
			style={{ width: 300 }}
			loading
		/>
	);

	// format the data 
	const data = mergeRowAndHeaderData(forms?.header, forms?.table || []);

	// format options
	const options = data.map((d) => {
		const entity = getEntityInfo(forms, d.form_urn);
		return ({
			value: d.form_urn,
			label: entity?.info?.name || d.form_urn,
		})
	});

	// Reset load states when form is changed
	const handleSetForm = (value) => {
		setSelectedForm(value);
		resetLoadStates();
	};

	return (
		<StyledSelect
			showSearch
			filterOption
			placeholder="Select a form"
			optionFilterProp="label"
			onChange={handleSetForm}
			options={options}
			defaultValue={options[0].value}
			size="large"
			style={{ width: 300 }}
		/>
	);
}	