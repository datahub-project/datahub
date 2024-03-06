import React from 'react';

import { Select } from 'antd';

import { mergeRowAndHeaderData, getEntityInfo } from './utils';

import { useFormAnalyticsContext } from './FormAnalyticsContext';

export const ByDomainSelector = () => {
	const {
		byDomain: { domains, hasDomains, setSelectedDomain },
		sectionLoadStates: { resetLoadStates }
	} = useFormAnalyticsContext();

	// If theres no forms, return null
	if (!hasDomains) return (
		<Select
			options={[{ value: 'loading', label: 'Loading...' }]}
			size="large"
			style={{ width: 300 }}
			loading
		/>
	);

	// format the data 
	const data = mergeRowAndHeaderData(domains?.header, domains?.table || []);

	// format options
	const options = data.map((d) => {
		const { properties } = getEntityInfo(domains, d.domain);
		return ({
			value: d.domain,
			label: properties?.name || d.domain
		})
	});

	// Reset load states when form is changed
	const handleSetDomain = (value: string) => {
		setSelectedDomain(value);
		resetLoadStates();
	};

	return (
		<Select
			showSearch
			filterOption
			placeholder="Select a domain"
			optionFilterProp="label"
			onChange={handleSetDomain}
			options={options}
			defaultValue={options[0].value}
			size="large"
			style={{ width: 300 }}
		/>
	);
}	