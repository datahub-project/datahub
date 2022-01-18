import { Button, Tag } from 'antd';
import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';
import { EntityType, FacetFilterInput, RecommendationContent, SearchParams } from '../../../../types.generated';
import { navigateToSearchUrl } from '../../../search/utils/navigateToSearchUrl';

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
