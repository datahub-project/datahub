import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../../../conf/Global';
import { RecommendationContent, Tag } from '../../../../types.generated';
import { StyledTag } from '../../../entity/shared/components/styled/StyledTag';
import { urlEncodeUrn } from '../../../entity/shared/utils';

const TagSearchListContainer = styled.div``;

type Props = {
    content: Array<RecommendationContent>;
};

export const TagSearchList = ({ content }: Props) => {
    const tags: Array<Tag> = content
        .map((cnt) => cnt.entity)
        .filter((entity) => entity !== null && entity !== undefined)
        .map((entity) => entity as Tag);
    return (
        <TagSearchListContainer>
            {tags.map((tag) => (
                <Link
                    to={{ pathname: `${PageRoutes.SEARCH}`, search: `?filter_tags=${urlEncodeUrn(tag.urn)}` }}
                    key={tag.urn}
                >
                    <StyledTag $colorHash={tag.urn} closable={false}>
                        {tag.name}
                    </StyledTag>
                </Link>
            ))}
        </TagSearchListContainer>
    );
};
