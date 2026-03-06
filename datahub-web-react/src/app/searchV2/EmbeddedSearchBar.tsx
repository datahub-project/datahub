import { MagnifyingGlass } from '@phosphor-icons/react';
import { Input } from 'antd';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components/macro';

import useGetSearchQueryInputs from '@app/searchV2/useGetSearchQueryInputs';
import filterSearchQuery from '@app/searchV2/utils/filterSearchQuery';

const SearchIcon = styled(MagnifyingGlass)`
    color: ${(props) => props.theme.colors.icon};
`;

const SearchInput = styled(Input)`
    width: 400px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
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
