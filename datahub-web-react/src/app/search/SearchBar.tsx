import React from 'react';
import { Input, AutoComplete } from 'antd';
import styled from 'styled-components';
import { AutoCompleteResultForEntity, EntityType } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import EntityRegistry from '../entity/EntityRegistry';

const SuggestionContainer = styled.div`
    display: 'flex',
    flex-direction: 'row',
    align-items: 'center',
`;

const SuggestionText = styled.span`
    margin-left: 10px;
`;

const styles = {
    autoComplete: { width: 650 },
};

const renderItem = (suggestion: string, icon: JSX.Element) => ({
    value: suggestion,
    label: (
        <SuggestionContainer>
            <span>{icon}</span>
            <SuggestionText>{suggestion}</SuggestionText>
        </SuggestionContainer>
    ),
});

interface Props {
    initialQuery?: string;
    placeholderText: string;
    suggestions: Array<AutoCompleteResultForEntity>;
    onSearch: (query: string) => void;
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
    const options = suggestions.map((entity: AutoCompleteResultForEntity) => ({
        label: Object.keys(EntityType).find((key) => EntityType[key] === entity.type) || entity.type,
        options: entity.suggestions.map((suggestion: string) =>
            renderItem(suggestion, entityRegistry.getIcon(entity.type, 14, IconStyleType.TAB_VIEW)),
        ),
    }));
    return (
        <div style={style}>
            <AutoComplete
                style={autoCompleteStyle || styles.autoComplete}
                options={options}
                onSelect={(value: string) => onSearch(value)}
                onSearch={(value: string) => onQueryChange(value)}
                defaultValue={initialQuery || undefined}
            >
                <Input.Search
                    placeholder={placeholderText}
                    onSearch={(value: string) => onSearch(value)}
                    data-testid="search-input"
                />
            </AutoComplete>
        </div>
    );
};

SearchBar.defaultProps = defaultProps;
