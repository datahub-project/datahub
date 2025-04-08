import { Skeleton } from 'antd';
import React, { useContext } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { HorizontalListSkeletons } from '@app/homeV2/content/HorizontalListSkeletons';
import { Section } from '@app/homeV2/content/tabs/discovery/sections/Section';
import { PlatformCard } from '@app/homeV2/content/tabs/discovery/sections/platform/PlatformCard';
import { useGetPlatforms } from '@app/homeV2/content/tabs/discovery/sections/platform/useGetPlatforms';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { HOME_PAGE_PLATFORMS_ID } from '@app/onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '@app/onboarding/useUpdateEducationStepsAllowList';
import { Carousel } from '@app/sharedV2/carousel/Carousel';

const SkeletonCard = styled(Skeleton.Button)<{ width: string }>`
    &&& {
        height: 83px;
        width: 180px;
    }
`;

export const Platforms = () => {
    const { user } = useUserContext();
    const { platforms, loading } = useGetPlatforms(user);
    const { isUserInitializing } = useContext(OnboardingContext);

    useUpdateEducationStepsAllowList(!!platforms.length, HOME_PAGE_PLATFORMS_ID);

    const showSkeleton = isUserInitializing || !user || loading;
    return (
        <div id={HOME_PAGE_PLATFORMS_ID}>
            {showSkeleton && <HorizontalListSkeletons Component={SkeletonCard} />}
            {!showSkeleton && !!platforms.length && (
                <Section title="Platforms">
                    <Carousel>
                        {platforms.map((platform) => (
                            <PlatformCard
                                key={platform.platform.urn}
                                platform={platform.platform}
                                count={platform.count}
                            />
                        ))}
                    </Carousel>
                </Section>
            )}
        </div>
    );
};
