import React from 'react';
import { Link } from 'react-router-dom';
import { PageRoutes } from '../../../conf/Global';
import { RecommendationContent, RecommendationRenderType, Tag } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { urlEncodeUrn } from '../../entity/shared/utils';
import { RecommendationDisplayType, RecommendationsRenderer } from './RecommendationsRenderer';

export class TagSearchListRenderer implements RecommendationsRenderer {
    entityRegistry;

    constructor(entityRegistry: EntityRegistry) {
        this.entityRegistry = entityRegistry;
    }

    renderRecommendation(
        _: string,
        _1: RecommendationRenderType,
        content: RecommendationContent[],
        _2: RecommendationDisplayType,
    ): JSX.Element {
        console.log(this.entityRegistry);
        const tags: Array<Tag> = content
            .map((cnt) => cnt.entity)
            .filter((entity) => entity !== null && entity !== undefined)
            .map((entity) => entity as Tag);
        return (
            <>
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
            </>
        );
    }
}
