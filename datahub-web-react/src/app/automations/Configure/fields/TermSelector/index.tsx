/*
* Resuable Term Selector Component
* Please keep this agnostic and reusable
*/

import React from 'react';

import { Select } from 'antd';

import { useGetSearchResultsQuery } from '../../../../../graphql/search.generated';
import { EntityType } from '../../../../../types.generated';

import { TagTermLabel } from '../../../../shared/tags/TagTermLabel';

// Clean data 
const cleanData = (data: any) =>
	data?.search?.searchResults?.map((searchResult: any) => searchResult.entity as any) || [];

// Util to simplify term data
const termDataForSelect = (terms: any, nodes: any) => ([
	{
		label: 'Glossary Terms',
		options: terms.map((term: any) => ({
			label: <TagTermLabel entity={term.entity} termName={term.properties?.name} />,
			value: term.urn
		})),
	},
	{
		label: 'Glossary Nodes',
		options: nodes.map((term: any) => ({
			label: <TagTermLabel entity={term.entity} termName={term.properties?.name} />,
			value: term.urn
		})),
	}
]);

// Component
export const TermSelector = ({ setTermsSelected }: any) => {
	// Get glossary terms
	const { data: glossaryTermData, loading: termLoading, error: termError } = useGetSearchResultsQuery({
		variables: {
			input: {
				query: '',
				type: EntityType.GlossaryTerm,
				filters: [], // TODO: Add filter for terms in existing recipe
				start: 0,
				count: 50,
			},
		},
	});

	// Get glossary nodes
	const { data: glossaryNodeData, loading: nodeLoading, error: nodeError } = useGetSearchResultsQuery({
		variables: {
			input: {
				query: '',
				type: EntityType.GlossaryNode,
				filters: [], // TODO: Add filter for terms in existing recipe
				start: 0,
				count: 50,
			},
		},
	});

	// Combined loading state
	const isLoading = termLoading || nodeLoading;
	const isError = termError || nodeError ? 'error' : undefined;

	// Clean up data
	const glossaryTerms = cleanData(glossaryTermData);
	const glossaryNodes = cleanData(glossaryNodeData);

	// Formatted terms for select
	const terms = termDataForSelect(glossaryTerms, glossaryNodes);

	return (
		<Select
			mode="tags"
			options={terms}
			loading={isLoading}
			status={isError}
			onChange={(value) => setTermsSelected(value)}
			placeholder="Select Terms…"
			allowClear
			showArrow
			showSearch
		/>
	);
};