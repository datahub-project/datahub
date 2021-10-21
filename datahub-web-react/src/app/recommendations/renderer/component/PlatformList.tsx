import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../../../conf/Global';
import { DataPlatform, RecommendationContent } from '../../../../types.generated';
import { urlEncodeUrn } from '../../../entity/shared/utils';
import { PlatformCard } from './PlatformCard';

const PlatformListContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

type Props = {
    content: Array<RecommendationContent>;
    onClick?: (index: number) => void;
};

export const PlatformList = ({ content, onClick }: Props) => {
    const platformsWithCounts: Array<{ platform: DataPlatform; count?: number }> = content
        .map((cnt) => ({ platform: cnt.entity, count: cnt.params?.contentParams?.count }))
        .filter(
            (platformWithCount) => platformWithCount.platform !== null && platformWithCount !== undefined,
        ) as Array<{ platform: DataPlatform; count?: number }>;
    return (
        <PlatformListContainer>
            {platformsWithCounts.map((platform, index) => (
                <Link
                    to={{
                        pathname: `${PageRoutes.SEARCH}`,
                        search: `?filter_platform=${urlEncodeUrn(platform.platform.urn)}`,
                    }}
                    key={platform.platform.urn}
                    onClick={() => onClick?.(index)}
                >
                    <PlatformCard
                        name={platform.platform.info?.displayName || ''}
                        logoUrl={platform.platform.info?.logoUrl || ''}
                        count={platform.count}
                    />
                </Link>
            ))}
        </PlatformListContainer>
    );
};
