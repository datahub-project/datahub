import { Skeleton } from 'antd';
import React, { useContext } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType, HomePageModule } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { HorizontalListSkeletons } from '@app/homeV2/content/HorizontalListSkeletons';
import { Section } from '@app/homeV2/content/tabs/discovery/sections/Section';
import { DomainCard } from '@app/homeV2/content/tabs/discovery/sections/domains/DomainCard';
import { useGetDomains } from '@app/homeV2/content/tabs/discovery/sections/domains/useGetDomains';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { HOME_PAGE_DOMAINS_ID } from '@app/onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '@app/onboarding/useUpdateEducationStepsAllowList';
import { Carousel } from '@app/sharedV2/carousel/Carousel';
import { PageRoutes } from '@conf/Global';

const SkeletonCard = styled(Skeleton.Button)<{ width: string }>`
    &&& {
        height: 83px;
        width: 287px;
    }
`;

export const Domains = () => {
    const history = useHistory();
    const { user } = useUserContext();
    const { isUserInitializing } = useContext(OnboardingContext);
    const { domains, loading } = useGetDomains(user);

    useUpdateEducationStepsAllowList(!!domains.length, HOME_PAGE_DOMAINS_ID);

    const navigateToDomains = () => {
        analytics.event({
            type: EventType.HomePageClick,
            module: HomePageModule.Discover,
            section: 'Domains',
            value: 'View all',
        });
        history.push(PageRoutes.DOMAINS);
    };

    const handleDomainClick = (domainUrn: string) => {
        analytics.event({
            type: EventType.HomePageClick,
            module: HomePageModule.Discover,
            section: 'Domains',
            value: domainUrn,
        });
    };

    const showSkeleton = isUserInitializing || !user || loading;
    return (
        <div id={HOME_PAGE_DOMAINS_ID}>
            {showSkeleton && <HorizontalListSkeletons Component={SkeletonCard} />}
            {!showSkeleton && !!domains.length && (
                <Section title="Domains" actionText="View all" onClickAction={navigateToDomains}>
                    <Carousel>
                        {domains.map((domain) => (
                            // eslint-disable-next-line
                            <span key={domain.entity.urn} onClick={() => handleDomainClick(domain.entity.urn)}>
                                <DomainCard domain={domain.entity} assetCount={domain.assetCount} />
                            </span>
                        ))}
                    </Carousel>
                </Section>
            )}
        </div>
    );
};
