/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { urlEncodeUrn } from '@app/entity/shared/utils';
import { LogoCountCard } from '@app/shared/LogoCountCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { PageRoutes } from '@conf/Global';

import { DataPlatform, RecommendationContent } from '@types';

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
