import React, { useContext, useState } from 'react';
import OnboardingContext from '../../../../onboarding/OnboardingContext';
import { ReferenceSectionProps } from '../../types';
import { ReferenceSection } from '../../../layout/shared/styledComponents';
import { useGetPinnedLinks } from './useGetPinnedLinks';
import { PinnedLinkList } from './PinnedLinkList';
import { useAppConfig } from '../../../../useAppConfig';
import { EntityLinkListSkeleton } from '../EntityLinkListSkeleton';

const DEFAULT_MAX_LINKS_TO_SHOW = 3;

export const PinnedLinks = ({ hideIfEmpty }: ReferenceSectionProps) => {
    const { isUserInitializing } = useContext(OnboardingContext);
    const [linkCount, setLinkCount] = useState(DEFAULT_MAX_LINKS_TO_SHOW);
    const { links, loading } = useGetPinnedLinks();
    const appConfig = useAppConfig();

    if (!appConfig.loaded || isUserInitializing || loading) {
        return <EntityLinkListSkeleton />;
    }

    if (hideIfEmpty && links.length === 0) {
        return null;
    }

    return (
        <ReferenceSection>
            <PinnedLinkList
                links={links.slice(0, linkCount)}
                title="Pinned links"
                tip="Pinned links for your organization"
                showMore={links.length > linkCount}
                onClickMore={() => setLinkCount(linkCount + DEFAULT_MAX_LINKS_TO_SHOW)}
                showMoreCount={
                    linkCount + DEFAULT_MAX_LINKS_TO_SHOW > links.length
                        ? links.length - linkCount
                        : DEFAULT_MAX_LINKS_TO_SHOW
                }
            />
        </ReferenceSection>
    );
};
