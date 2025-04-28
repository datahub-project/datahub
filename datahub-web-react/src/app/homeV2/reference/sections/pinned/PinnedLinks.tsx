import React, { useContext } from 'react';

import { EntityLinkListSkeleton } from '@app/homeV2/reference/sections/EntityLinkListSkeleton';
import { PinnedLinkList } from '@app/homeV2/reference/sections/pinned/PinnedLinkList';
import { useGetPinnedLinks } from '@app/homeV2/reference/sections/pinned/useGetPinnedLinks';
import { ReferenceSectionProps } from '@app/homeV2/reference/types';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { useAppConfig } from '@app/useAppConfig';
import { Section } from '@src/app/homeV2/content/tabs/discovery/sections/Section';

export const PinnedLinks = ({ hideIfEmpty }: ReferenceSectionProps) => {
    const { isUserInitializing } = useContext(OnboardingContext);
    const { links, loading } = useGetPinnedLinks();
    const appConfig = useAppConfig();

    if (!appConfig.loaded || isUserInitializing || loading) {
        return <EntityLinkListSkeleton />;
    }

    if (hideIfEmpty && links.length === 0) {
        return null;
    }

    return (
        <Section title="Pinned Links" tip="Links pinned by your DataHub admins">
            <PinnedLinkList links={links} />
        </Section>
    );
};
