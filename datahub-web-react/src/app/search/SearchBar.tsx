import React, { useEffect, useState } from 'react';
import { Input, AutoComplete, Select } from 'antd';

const { Search } = Input;
const { Option } = Select;

interface Props {
    types: Array<string>;
    selectedType: string;
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
    types,
    selectedType,
    initialQuery,
    placeholderText,
    suggestions,
    onSearch,
    onQueryChange,
    style,
}: Props) => {
    const [activeType, setActiveType] = useState(selectedType);

    useEffect(() => {
        setActiveType(selectedType);
    }, [selectedType]);

    const onTypeChange = (value: string) => {
        setActiveType(value);
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
                ...style,
            }}
        >
            <Select value={activeType} style={{ marginRight: '12px', width: 250 }} onChange={onTypeChange}>
                {types.map((t) => (
                    <Option value={t}>{t}</Option>
                ))}
            </Select>
            <AutoComplete
                style={{ width: 500 }}
                options={suggestions.map((result: string) => ({ value: result }))}
                onSelect={(value: string) => onSearch(activeType, value)}
                onSearch={(value: string) => onQueryChange(activeType, value)}
                defaultValue={initialQuery}
            >
                <Search placeholder={placeholderText} onSearch={(value: string) => onSearch(activeType, value)} />
            </AutoComplete>
        </div>
    );
};

SearchBar.defaultProps = defaultProps;
