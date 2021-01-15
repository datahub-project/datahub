import React, { useState } from 'react';
import { Input, AutoComplete, Select } from 'antd';

const { Search } = Input;
const { Option } = Select;

interface Props {
    types: Array<string>;
    initialType: string;
    initialQuery: string;
    placeholderText: string;
    suggestions: Array<string>;
    onSearch: (type: string, query: string) => void;
    onQueryChange: (type: string, query: string) => void;
    style?: React.CSSProperties;
}

const defaultProps = {
    style: undefined,
};

/**
 * Represents the search bar appearing in the default header view.
 */
export const SearchBar = ({
    types: _types,
    initialType: _initialType,
    initialQuery: _initialQuery,
    placeholderText: _placeholderText,
    suggestions: _suggestions,
    onSearch: _onSearch,
    onQueryChange: _onQueryChange,
    style: _style,
}: Props) => {
    const [type, setType] = useState(_initialType);

    const onTypeChange = (value: string) => {
        setType(value);
    };

    return (
        <div
            style={{
                height: '64px',
                width: '900px',
                padding: '0px 40px',
                margin: '0px auto',
                display: 'flex',
                alignItems: 'center',
                ..._style,
            }}
        >
            <Select defaultValue={_initialType} style={{ marginRight: '12px', width: 250 }} onChange={onTypeChange}>
                {_types.map((t) => (
                    <Option value={t}>{t}</Option>
                ))}
            </Select>
            <AutoComplete
                style={{ width: 500 }}
                options={_suggestions.map((result: string) => ({ value: result }))}
                onSelect={(value: string) => _onSearch(type, value)}
                onSearch={(value: string) => _onQueryChange(type, value)}
                defaultValue={_initialQuery}
            >
                <Search placeholder={_placeholderText} onSearch={(value: string) => _onSearch(type, value)} />
            </AutoComplete>
        </div>
    );
};

SearchBar.defaultProps = defaultProps;
