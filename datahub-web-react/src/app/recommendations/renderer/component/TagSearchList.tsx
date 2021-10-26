import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../../../conf/Global';
import { RecommendationContent, Tag } from '../../../../types.generated';
import { StyledTag } from '../../../entity/shared/components/styled/StyledTag';
import { urlEncodeUrn } from '../../../entity/shared/utils';

const TagSearchListContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
`;

const TagContainer = styled.div`
    margin-bottom: 4px;
`;

type Props = {
    content: Array<RecommendationContent>;
    onClick?: (index: number) => void;
};

export const TagSearchList = ({ content, onClick }: Props) => {
    const tags: Array<Tag> = content
        .map((cnt) => cnt.entity)
        .filter((entity) => entity !== null && entity !== undefined)
        .map((entity) => entity as Tag);
    return (
        <TagSearchListContainer>
            {tags.map((tag, index) => (
                <Link
                    to={{ pathname: `${PageRoutes.SEARCH}`, search: `?filter_tags=${urlEncodeUrn(tag.urn)}` }}
                    key={tag.urn}
                    onClick={() => onClick?.(index)}
                >
                    <TagContainer>
                        <StyledTag $colorHash={tag.urn} closable={false}>
                            {tag.name}
                        </StyledTag>
                    </TagContainer>
                </Link>
            ))}
        </TagSearchListContainer>
    );
};
