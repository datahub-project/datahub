import React from 'react';
import { Input, AutoComplete } from 'antd';

const { Search } = Input;

const styles = {
    autoComplete: { width: 650 },
};

interface Props {
    initialQuery: string;
    placeholderText: string;
    suggestions: Array<string>;
    onSearch: (query: string) => void;
    onQueryChange: (query: string) => void;
    style?: React.CSSProperties;
}

const defaultProps = {
    style: undefined,
};

/**
 * Represents the search bar appearing in the default header view.
 */
export const SearchBar = ({ initialQuery, placeholderText, suggestions, onSearch, onQueryChange, style }: Props) => {
    return (
        <div style={style}>
            <AutoComplete
                style={styles.autoComplete}
                options={suggestions.map((result: string) => ({ value: result }))}
                onSelect={(value: string) => onSearch(value)}
                onSearch={(value: string) => onQueryChange(value)}
                defaultValue={initialQuery}
            >
                <Search placeholder={placeholderText} onSearch={(value: string) => onSearch(value)} />
            </AutoComplete>
        </div>
    );
};

SearchBar.defaultProps = defaultProps;
