import { Select } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

type Props = {
    placeholder?: string;
    /** Single-pick mode: called with the picked tag / glossary term entity; the select then clears. */
    onPick?: (entity: Entity) => void;
    /** Multi mode (e.g. filter values): controlled urns. Set `onChangeUrns` to enable. */
    selectedUrns?: string[];
    onChangeUrns?: (urns: string[], entities: Entity[]) => void;
    size?: 'small' | 'middle';
};

const LABEL_TYPES = [EntityType.Tag, EntityType.GlossaryTerm];
const LIMIT = 10;

/**
 * Search-and-pick tags / glossary terms, on the same `autoCompleteForMultiple` query the entity
 * selectors (DomainSelector, SetDataProductModal, EntitySearchInputV2) use. Two modes:
 *  - `onPick`: one entity per pick, for LABEL columns (the select clears after each pick)
 *  - `selectedUrns` + `onChangeUrns`: a controlled multi-select, for `tags` / `glossaryTerms`
 *    filter values
 * Entities seen so far are cached so selected chips keep their names as the search narrows.
 */
export default function LabelPicker({ placeholder, onPick, selectedUrns, onChangeUrns, size = 'small' }: Props) {
    const registry = useEntityRegistry();
    const [text, setText] = useState('');
    const [autoComplete, { data, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const [seen, setSeen] = useState<Record<string, Entity>>({});

    useDebounce(
        () => {
            const query = text.trim();
            if (query) autoComplete({ variables: { input: { types: LABEL_TYPES, query, limit: LIMIT } } });
        },
        200,
        [text],
    );

    const results: Entity[] = useMemo(
        () => (data?.autoCompleteForMultiple?.suggestions || []).flatMap((s) => s.entities as Entity[]),
        [data],
    );
    useEffect(() => {
        if (results.length) setSeen((prev) => ({ ...prev, ...Object.fromEntries(results.map((e) => [e.urn, e])) }));
    }, [results]);

    const multi = Boolean(onChangeUrns);
    const optionEntities = useMemo(() => {
        const byUrn = new Map<string, Entity | undefined>();
        (selectedUrns || []).forEach((urn) => byUrn.set(urn, seen[urn]));
        results.forEach((e) => byUrn.set(e.urn, e));
        return Array.from(byUrn.entries());
    }, [results, seen, selectedUrns]);

    const optionLabel = (urn: string, e?: Entity) =>
        e ? `${registry.getDisplayName(e.type, e)} (${e.type === EntityType.Tag ? 'tag' : 'term'})` : urn;

    return (
        <Select
            mode={multi ? 'multiple' : undefined}
            showSearch
            size={size}
            style={{ width: '100%' }}
            placeholder={placeholder}
            value={multi ? selectedUrns || [] : (null as any)}
            loading={loading}
            filterOption={false}
            searchValue={text}
            onSearch={setText}
            onChange={(value) => {
                if (multi) {
                    const urns = value as string[];
                    onChangeUrns?.(urns, urns.map((u) => seen[u]).filter(Boolean) as Entity[]);
                } else {
                    const picked = results.find((e) => e.urn === value) || seen[value as string];
                    if (picked) onPick?.(picked);
                }
                setText('');
            }}
            options={optionEntities.map(([urn, e]) => ({ value: urn, label: optionLabel(urn, e) }))}
        />
    );
}
