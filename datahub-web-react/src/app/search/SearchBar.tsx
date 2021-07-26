import React, { useState } from 'react';
import { Input, AutoComplete } from 'antd';
import styled from 'styled-components';
import { AutoCompleteResultForEntity, EntityType } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import EntityRegistry from '../entity/EntityRegistry';
import { SEARCH_FOR_ENTITY_PREFIX } from './utils/constants';
import filterSearchQuery from './utils/filterSearchQuery';

const SuggestionContainer = styled.div`
    display: 'flex',
    flex-direction: 'row',
    align-items: 'center',
`;

const SuggestionText = styled.span`
    margin-left: 10px;
`;

const ExploreForEntity = styled.span`
    font-style: italic;
    font-weight: light;
    font-size: 11px;
`;

const StyledAutoComplete = styled(AutoComplete)`
    width: 100%;
    max-width: 800px;
`;

const AutoCompleteContainer = styled.div`
    width: 100%;
    text-align: center;
    padding: 0 30px;
`;

const renderItem = (suggestion: string, icon: JSX.Element, type: string) => ({
    value: suggestion,
    label: (
        <SuggestionContainer>
            <span>{icon}</span>
            <SuggestionText>{suggestion}</SuggestionText>
        </SuggestionContainer>
    ),
    type,
});

interface Props {
    initialQuery?: string;
    placeholderText: string;
    suggestions: Array<AutoCompleteResultForEntity>;
    onSearch: (query: string, type?: EntityType) => void;
    onQueryChange: (query: string) => void;
    style?: React.CSSProperties;
    autoCompleteStyle?: React.CSSProperties;
    entityRegistry: EntityRegistry;
}

const defaultProps = {
    style: undefined,
};

/**
 * Represents the search bar appearing in the default header view.
 */
export const SearchBar = ({
    initialQuery,
    placeholderText,
    suggestions,
    onSearch,
    onQueryChange,
    entityRegistry,
    style,
    autoCompleteStyle,
}: Props) => {
    const [searchQuery, setSearchQuery] = useState<string>();
    const [selected, setSelected] = useState<string>();
    const options = suggestions.map((entity: AutoCompleteResultForEntity) => ({
        label: Object.keys(EntityType).find((key) => EntityType[key] === entity.type) || entity.type,
        options: [
            ...entity.suggestions.map((suggestion: string) =>
                renderItem(suggestion, entityRegistry.getIcon(entity.type, 14, IconStyleType.TAB_VIEW), entity.type),
            ),
            {
                value: `${SEARCH_FOR_ENTITY_PREFIX}${searchQuery}__${entity.type}`,
                label: (
                    <SuggestionContainer key={entity.type}>
                        <ExploreForEntity>
                            Explore all `{searchQuery}` in {entityRegistry.getCollectionName(entity.type)}
                        </ExploreForEntity>
                    </SuggestionContainer>
                ),
                type: entity.type,
                key: `${searchQuery}-${entity.type}`,
            },
        ],
    }));

    return (
        <AutoCompleteContainer style={style}>
            <StyledAutoComplete
                style={autoCompleteStyle}
                options={options}
                onSelect={(value: string, option) => onSearch(filterSearchQuery(value), option.type)}
                onSearch={(value: string) => onQueryChange(value)}
                defaultValue={initialQuery || undefined}
                value={selected}
                onChange={(v) => setSelected(filterSearchQuery(v))}
                dropdownStyle={{ maxHeight: 1000, overflowY: 'visible' }}
            >
                <Input.Search
                    placeholder={placeholderText}
                    onSearch={(value: string) => onSearch(filterSearchQuery(value))}
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    data-testid="search-input"
                />
            </StyledAutoComplete>
        </AutoCompleteContainer>
    );
};

SearchBar.defaultProps = defaultProps;
