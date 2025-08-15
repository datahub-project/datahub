import { Button } from 'antd';
import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { StyledTag } from '@app/entity/shared/components/styled/StyledTag';
import { UnionType } from '@app/search/utils/constants';
import { navigateToSearchUrl } from '@app/search/utils/navigateToSearchUrl';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, RecommendationContent, Tag } from '@types';

const TagSearchListContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
`;

const TagContainer = styled.div`
    margin-bottom: 4px;
`;

const TagButton = styled(Button)`
    margin: 0px;
    padding: 0px;
    font-weight: 500;
`;

type Props = {
    content: Array<RecommendationContent>;
    onClick?: (index: number) => void;
};

export const TagSearchList = ({ content, onClick }: Props) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();

    const tags: Array<Tag> = content
        .map((cnt) => cnt.entity)
        .filter((entity) => entity !== null && entity !== undefined)
        .map((entity) => entity as Tag);

    const onClickTag = (tag: any, index: number) => {
        onClick?.(index);
        navigateToSearchUrl({
            filters: [
                {
                    field: 'tags',
                    values: [tag.urn],
                },
                {
                    field: 'fieldTags',
                    values: [tag.urn],
                },
            ],
            history,
            unionType: UnionType.OR,
        });
    };

    return (
        <TagSearchListContainer>
            {tags.map((tag, index) => (
                <TagContainer key={tag.urn}>
                    <TagButton type="link" onClick={() => onClickTag(tag, index)}>
                        <StyledTag $colorHash={tag?.urn} $color={tag?.properties?.colorHex} closable={false}>
                            {entityRegistry.getDisplayName(EntityType.Tag, tag)}
                        </StyledTag>
                    </TagButton>
                </TagContainer>
            ))}
        </TagSearchListContainer>
    );
};
