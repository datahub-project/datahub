import React from 'react';

import { Select } from 'antd';

import { mergeRowAndHeaderData, getEntityInfo } from './utils';

import { useFormAnalyticsContext } from './FormAnalyticsContext';

export const ByDomainSelector = () => {
	const { byDomain: { domains, hasDomains, setSelectedDomain } } = useFormAnalyticsContext();

	console.log(domains);

	// If theres no forms, return null
	if (!hasDomains) return null;

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

	return (
		<Select
			showSearch
			filterOption
			placeholder="Select a domain"
			optionFilterProp="label"
			onChange={(value: string) => setSelectedDomain(value)}
			options={options}
			defaultValue={options[0].value}
			size="large"
			style={{ width: 300, marginBottom: '2rem' }}
		/>
	);
}	