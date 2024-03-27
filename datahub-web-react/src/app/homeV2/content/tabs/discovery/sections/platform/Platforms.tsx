import React from 'react';
import { Section } from '../Section';
import { PlatformCard } from './PlatformCard';
import { useGetPlatforms } from './useGetPlatforms';
import { useUserContext } from '../../../../../../context/useUserContext';
import { HOME_PAGE_PLATFORMS_ID } from '../../../../../../onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '../../../../../../onboarding/useUpdateEducationStepsAllowList';
import { Carousel } from '../../../../../../sharedV2/carousel/Carousel';

export const Platforms = () => {
    const { user } = useUserContext();
    const platforms = useGetPlatforms(user);

    useUpdateEducationStepsAllowList(!!platforms.length, HOME_PAGE_PLATFORMS_ID);

    return (
        <div id={HOME_PAGE_PLATFORMS_ID}>
            {(platforms.length && (
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
            )) ||
                null}
        </div>
    );
};
