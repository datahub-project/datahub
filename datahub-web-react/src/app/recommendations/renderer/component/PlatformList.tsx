import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../../../conf/Global';
import { DataPlatform, RecommendationContent } from '../../../../types.generated';
import { urlEncodeUrn } from '../../../entity/shared/utils';
import { LogoCountCard } from '../../../shared/LogoCountCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';

const PlatformListContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
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
                    <LogoCountCard
                        name={
                            platform.platform.properties?.displayName ||
                            capitalizeFirstLetterOnly(platform.platform.name) ||
                            ''
                        }
                        logoUrl={platform.platform.properties?.logoUrl || ''}
                        count={platform.count}
                    />
                </Link>
            ))}
        </PlatformListContainer>
    );
};
