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
};

export const PlatformList = ({ content }: Props) => {
    const platforms: Array<DataPlatform> = content
        .map((cnt) => cnt.entity)
        .filter((platform) => platform !== null && platform !== undefined) as Array<DataPlatform>;
    return (
        <PlatformListContainer>
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
        </PlatformListContainer>
    );
};
