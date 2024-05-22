/*
* Resuable Term Selector Component
* Please keep this agnostic and reusable
*/

import React from 'react';

import { Select } from 'antd';

import { useListIngestionSourcesQuery } from '../../../../../graphql/ingestion.generated';

export const ConnectionSelector = ({ connectionTypes, setConnectionSelected }: any) => {
	const { data, loading, error } = useListIngestionSourcesQuery({
		variables: {
			input: {
				// TODO: Figure out how to filter by connection type
				// filters: [
				// 	{
				// 		field: 'type', values: [connectionTypes],
				// 	}
				// ]
			},
		},
	});

	const sources = data?.listIngestionSources?.ingestionSources || [];
	const isError = error ? 'error' : undefined;

	// Filter & map data
	const connections = sources
		.filter((source) => connectionTypes.includes(source.type)) // Doing this because API filter isn't working
		.map((source) => ({ label: source.name, value: source.urn }));

	// Handle change for select
	const handleChange = (value) => setConnectionSelected(value);

	return (
		<Select
			options={connections}
			loading={loading}
			status={isError}
			onChange={handleChange}
			placeholder="Select Connection…"
			allowClear
			showArrow
		/>
	);
}