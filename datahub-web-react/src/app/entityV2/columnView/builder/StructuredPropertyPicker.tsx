import { Select, Typography } from 'antd';
import React, { useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import { getEntityTypesPropertyFilter, getNotHiddenPropertyFilter } from '@app/govern/structuredProperties/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, StructuredPropertyEntity } from '@types';

type Props = {
    selectedUrns: string[];
    /** Selected urns plus the matching entities (for those the picker has seen), so callers can show names pre-save. */
    onChange: (urns: string[], entities: StructuredPropertyEntity[]) => void;
    placeholder?: string;
};

const PAGE_SIZE = 50;

/**
 * Multi-select over the structured properties that can apply to schema fields: the same
 * `entityTypes` includes schemaField / not-hidden filters useGetTableColumnProperties uses (minus
 * the "show in columns table" flag, which a Column View overrides by construction). Server-side
 * search; each option shows the display name with the qualified name alongside.
 */
export default function StructuredPropertyPicker({ selectedUrns, onChange, placeholder }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const [text, setText] = useState('');
    const [query, setQuery] = useState('');
    useDebounce(() => setQuery(text.trim()), 200, [text]);

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.StructuredProperty],
                query: query || '*',
                start: 0,
                count: PAGE_SIZE,
                orFilters: [{ and: [getEntityTypesPropertyFilter(entityRegistry, true), getNotHiddenPropertyFilter()] }],
            },
        },
        fetchPolicy: 'cache-first',
    });

    // Remember every property seen so selected chips keep their names as the search narrows.
    const [seen, setSeen] = useState<Record<string, StructuredPropertyEntity>>({});
    const results = useMemo(
        () => (data?.searchAcrossEntities?.searchResults || []).map((r) => r.entity as StructuredPropertyEntity),
        [data],
    );
    React.useEffect(() => {
        if (results.length) setSeen((prev) => ({ ...prev, ...Object.fromEntries(results.map((e) => [e.urn, e])) }));
    }, [results]);

    const optionEntities = useMemo(() => {
        const byUrn = new Map<string, StructuredPropertyEntity | undefined>();
        selectedUrns.forEach((urn) => byUrn.set(urn, seen[urn]));
        results.forEach((e) => byUrn.set(e.urn, e));
        return Array.from(byUrn.entries());
    }, [results, seen, selectedUrns]);

    return (
        <Select
            mode="multiple"
            showSearch
            style={{ width: '100%' }}
            placeholder={placeholder}
            loading={loading}
            filterOption={false}
            searchValue={text}
            onSearch={setText}
            value={selectedUrns}
            onChange={(urns) => {
                const next = urns as string[];
                const byUrn = new Map(optionEntities);
                onChange(
                    next,
                    next.map((u) => byUrn.get(u)).filter((e): e is StructuredPropertyEntity => !!e),
                );
                setText('');
            }}
            optionLabelProp="title"
            options={optionEntities.map(([urn, e]) => {
                const displayName = e?.definition?.displayName || e?.definition?.qualifiedName || urn;
                const qualifiedName = e?.definition?.qualifiedName;
                return {
                    value: urn,
                    title: displayName,
                    label: (
                        <span>
                            {displayName}
                            {qualifiedName && qualifiedName !== displayName && (
                                <Typography.Text type="secondary" style={{ marginLeft: 6 }}>
                                    {qualifiedName}
                                </Typography.Text>
                            )}
                        </span>
                    ),
                };
            })}
        />
    );
}
