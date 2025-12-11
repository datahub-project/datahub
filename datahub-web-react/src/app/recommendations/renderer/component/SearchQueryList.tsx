/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Tag } from 'antd';
import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { navigateToSearchUrl } from '@app/search/utils/navigateToSearchUrl';

import { EntityType, FacetFilterInput, RecommendationContent, SearchParams } from '@types';

const SearchQueryListContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
`;

const TagContainer = styled.div`
    margin-bottom: 4px;
`;

const QueryButton = styled(Button)`
    margin: 0px;
    padding: 0px;
`;

type Props = {
    content: Array<RecommendationContent>;
    onClick?: (index: number) => void;
};

export const SearchQueryList = ({ content, onClick }: Props) => {
    const history = useHistory();

    const queryParams = content
        .map((cnt) => cnt.params?.searchParams)
        .filter((params) => params !== null && params !== undefined) as Array<SearchParams>;

    const onClickQuery = (query: string, types: Array<EntityType>, filters: Array<FacetFilterInput>, index: number) => {
        onClick?.(index);
        navigateToSearchUrl({
            query,
            type: (types && types.length > 0 && types[0]) || undefined,
            filters: filters || [],
            history,
        });
    };

    return (
        <SearchQueryListContainer>
            {queryParams.map((params, index) => (
                <QueryButton
                    type="text"
                    key={params.query}
                    onClick={() => onClickQuery(params.query, params.types || [], params.filters || [], index)}
                >
                    <TagContainer>
                        <Tag closable={false}>{params.query}</Tag>
                    </TagContainer>
                </QueryButton>
            ))}
        </SearchQueryListContainer>
    );
};
