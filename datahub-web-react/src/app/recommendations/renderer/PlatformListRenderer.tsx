import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../../conf/Global';
import { DataPlatform, RecommendationContent, RecommendationRenderType } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import { urlEncodeUrn } from '../../entity/shared/utils';
import { PlatformCard } from './PlatformCard';
import { RecommendationDisplayType, RecommendationsRenderer } from './RecommendationsRenderer';

const PlatformList = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

export class PlatformListRenderer implements RecommendationsRenderer {
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
        const platforms: Array<DataPlatform> = content
            .map((cnt) => cnt.entity)
            .filter((platform) => platform !== null && platform !== undefined) as Array<DataPlatform>;
        return (
            <PlatformList>
                {platforms.map((platform) => (
                    <Link
                        to={{
                            pathname: `${PageRoutes.SEARCH}`,
                            search: `?filter_platform=${urlEncodeUrn(platform.urn)}`,
                        }}
                        key={platform.urn}
                    >
                        <PlatformCard name={platform.info?.displayName || ''} logoUrl={platform.info?.logoUrl || ''} />
                    </Link>
                ))}
            </PlatformList>
        );
    }
}
