import { Select, Tag, Tooltip } from 'antd';
import React from 'react';
import { useGetSearchResultsForMultipleLazyQuery } from '../../../../graphql/search.generated';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntitySearchInputResult } from './EntitySearchInputResult';

type Props = {
    selectedUrns: string[];
    entityTypes: EntityType[];
    placeholder?: string;
    mode?: 'multiple' | 'single';
    style?: any;
    onChangeSelectedUrns: (newUrns: string[]) => void;
};

export const EntitySearchInput = ({
    selectedUrns,
    entityTypes,
    placeholder,
    style,
    mode,
    onChangeSelectedUrns,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const [searchResources, { data: resourcesSearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const searchResults = resourcesSearchData?.searchAcrossEntities?.searchResults || [];

    const urnToSearchResultEntity = new Map();
    searchResults.forEach((result) => {
        urnToSearchResultEntity[result.entity.urn] = {
            urn: result.entity.urn,
            type: result.entity.type,
            displayName: entityRegistry.getDisplayName(result.entity.type, result.entity),
        };
    });

    const onSelect = (newUrn) => {
        if (mode === 'single') {
            onChangeSelectedUrns([newUrn]);
        } else {
            const newUrns = [...selectedUrns, newUrn];
            onChangeSelectedUrns(newUrns);
        }
    };

    const onDeselect = (urn) => {
        if (mode === 'single') {
            onChangeSelectedUrns([]);
        } else {
            onChangeSelectedUrns(selectedUrns.filter((u) => u !== urn));
        }
    };

    const onSearch = (text: string) => {
        searchResources({
            variables: {
                input: {
                    types: entityTypes,
                    query: text,
                    start: 0,
                    count: 10,
                },
            },
        });
    };

    return (
        <Select
            value={selectedUrns}
            mode="multiple"
            style={style}
            filterOption={false}
            placeholder={placeholder || 'Search for entities...'}
            onSelect={onSelect}
            onDeselect={onDeselect}
            onSearch={onSearch}
            tagRender={(tagProps) => {
                const displayName = tagProps.value as string; // TODO: Support display name resolution.
                return (
                    <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                        <Tooltip title={displayName}>{displayName}</Tooltip>
                    </Tag>
                );
            }}
        >
            {searchResults?.map((result) => (
                <Select.Option value={result.entity.urn}>
                    <EntitySearchInputResult entity={result.entity} />
                </Select.Option>
            ))}
        </Select>
    );
};
