import React, { useContext } from 'react';
import { Section } from '@src/app/homeV2/content/tabs/discovery/sections/Section';
import OnboardingContext from '../../../../onboarding/OnboardingContext';
import { ReferenceSectionProps } from '../../types';
import { useGetPinnedLinks } from './useGetPinnedLinks';
import { PinnedLinkList } from './PinnedLinkList';
import { useAppConfig } from '../../../../useAppConfig';
import { EntityLinkListSkeleton } from '../EntityLinkListSkeleton';

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
