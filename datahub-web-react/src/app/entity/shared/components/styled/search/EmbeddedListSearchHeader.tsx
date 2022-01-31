import React from 'react';
import { Button, Typography } from 'antd';
import { FilterOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import TabToolbar from '../TabToolbar';
import { SearchBar } from '../../../../../search/SearchBar';
import { useEntityRegistry } from '../../../../../useEntityRegistry';

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-bottom: 16px;
    width: 100%;
`;

type Props = {
    onSearch: (q: string) => void;
    onToggleFilters: () => void;
    placeholderText?: string | null;
};

export default function EmbeddedListSearchHeader({ onSearch, onToggleFilters, placeholderText }: Props) {
    const entityRegistry = useEntityRegistry();

    const onQueryChange = (query: string) => {
        onSearch(query);
    };

    return (
        <TabToolbar>
            <HeaderContainer>
                <Button type="text" onClick={onToggleFilters}>
                    <FilterOutlined />
                    <Typography.Text>Filters</Typography.Text>
                </Button>
                <SearchBar
                    initialQuery=""
                    placeholderText={placeholderText || 'Search entities...'}
                    suggestions={[]}
                    style={{
                        maxWidth: 220,
                        padding: 0,
                    }}
                    inputStyle={{
                        height: 32,
                        fontSize: 12,
                    }}
                    onSearch={onSearch}
                    onQueryChange={onQueryChange}
                    entityRegistry={entityRegistry}
                />
            </HeaderContainer>
        </TabToolbar>
    );
}
