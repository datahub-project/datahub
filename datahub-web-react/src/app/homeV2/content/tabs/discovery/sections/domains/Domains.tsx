import { Skeleton } from 'antd';
import React, { useContext } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { Section } from '../Section';
import { DomainCard } from './DomainCard';
import { useGetDomains } from './useGetDomains';
import { useUserContext } from '../../../../../../context/useUserContext';
import { PageRoutes } from '../../../../../../../conf/Global';
import { HOME_PAGE_DOMAINS_ID } from '../../../../../../onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '../../../../../../onboarding/useUpdateEducationStepsAllowList';
import { Carousel } from '../../../../../../sharedV2/carousel/Carousel';
import { HorizontalListSkeletons } from '../../../../HorizontalListSkeletons';
import OnboardingContext from '../../../../../../onboarding/OnboardingContext';

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
        history.push(PageRoutes.DOMAINS);
    };

    const showSkeleton = isUserInitializing || !user || loading;
    return (
        <div id={HOME_PAGE_DOMAINS_ID}>
            {showSkeleton && <HorizontalListSkeletons Component={SkeletonCard} />}
            {!showSkeleton && !!domains.length && (
                <Section title="Domains" actionText="View all" onClickAction={navigateToDomains}>
                    <Carousel>
                        {domains.map((domain) => (
                            <DomainCard key={domain.entity.urn} domain={domain.entity} assetCount={domain.assetCount} />
                        ))}
                    </Carousel>
                </Section>
            )}
        </div>
    );
};
