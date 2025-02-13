import React, { useEffect, useState } from 'react';
import { Input } from 'antd';
import styled from 'styled-components/macro';
import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import { SearchOutlined } from '@ant-design/icons';
import filterSearchQuery from './utils/filterSearchQuery';
import useGetSearchQueryInputs from './useGetSearchQueryInputs';

const SearchIcon = styled(SearchOutlined)`
    color: #8088a3;
`;

const SearchInput = styled(Input)`
    width: 400px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    border-radius: 70px;
`;

interface Props {
    style?: React.CSSProperties;
}

export const EmbeddedSearchBar = ({ style }: Props) => {
    const history = useHistory();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const { query: urlQuery } = useGetSearchQueryInputs();

    const [searchQuery, setSearchQuery] = useState<string>(urlQuery);

    useEffect(() => {
        setSearchQuery(urlQuery);
    }, [urlQuery]);

    function handleSearch(query: string) {
        const search = QueryString.stringify(
            {
                ...params,
                query: encodeURIComponent(filterSearchQuery(query) || ''),
                page: 1,
            },
            { arrayFormat: 'comma' },
        );

        history.push({ search });
    }

    return (
        <SearchInput
            placeholder="Search Assets..."
            onChange={(e) => setSearchQuery(e.target.value)}
            onPressEnter={() => {
                handleSearch(searchQuery || '');
            }}
            value={searchQuery}
            allowClear
            style={style}
            prefix={
                <SearchIcon
                    onClick={() => {
                        handleSearch(searchQuery);
                    }}
                />
            }
        />
    );
};
