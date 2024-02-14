import React from 'react';
import { Section } from '../Section';
import { PlatformCard } from './PlatformCard';
import { useGetPlatforms } from './useGetPlatforms';
import { useUserContext } from '../../../../../../context/useUserContext';
import { HorizontalList } from '../../../../../layout/shared/styledComponents';
import { HOME_PAGE_PLATFORMS_ID } from '../../../../../../onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '../../../../../../onboarding/useUpdateEducationStepsAllowList';

export const Platforms = () => {
    const { user } = useUserContext();
    const platforms = useGetPlatforms(user);

    useUpdateEducationStepsAllowList(!!platforms.length, HOME_PAGE_PLATFORMS_ID);

    return (
        <div id={HOME_PAGE_PLATFORMS_ID}>
            {(platforms.length && (
                <Section title="Platforms">
                    <HorizontalList>
                        {platforms.map((platform) => (
                            <PlatformCard
                                key={platform.platform.urn}
                                platform={platform.platform}
                                count={platform.count}
                            />
                        ))}
                    </HorizontalList>
                </Section>
            )) ||
                null}
        </div>
    );
};
