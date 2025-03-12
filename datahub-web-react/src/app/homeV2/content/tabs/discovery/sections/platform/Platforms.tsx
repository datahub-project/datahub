import { Skeleton } from 'antd';
import React, { useContext } from 'react';
import styled from 'styled-components';
import { Section } from '../Section';
import { PlatformCard } from './PlatformCard';
import { useGetPlatforms } from './useGetPlatforms';
import { useUserContext } from '../../../../../../context/useUserContext';
import { HOME_PAGE_PLATFORMS_ID } from '../../../../../../onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '../../../../../../onboarding/useUpdateEducationStepsAllowList';
import { Carousel } from '../../../../../../sharedV2/carousel/Carousel';
import { HorizontalListSkeletons } from '../../../../HorizontalListSkeletons';
import OnboardingContext from '../../../../../../onboarding/OnboardingContext';

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
