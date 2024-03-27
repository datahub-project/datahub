import React from 'react';
import { useHistory } from 'react-router';
import { Section } from '../Section';
import { DomainCard } from './DomainCard';
import { useGetDomains } from './useGetDomains';
import { useUserContext } from '../../../../../../context/useUserContext';
import { PageRoutes } from '../../../../../../../conf/Global';
import { HOME_PAGE_DOMAINS_ID } from '../../../../../../onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '../../../../../../onboarding/useUpdateEducationStepsAllowList';
import { Carousel } from '../../../../../../sharedV2/carousel/Carousel';

export const Domains = () => {
    const history = useHistory();
    const { user } = useUserContext();
    const domains = useGetDomains(user);

    const navigateToDomains = () => {
        history.push(PageRoutes.DOMAINS);
    };

    useUpdateEducationStepsAllowList(!!domains.length, HOME_PAGE_DOMAINS_ID);

    return (
        <div id={HOME_PAGE_DOMAINS_ID}>
            {(domains.length && (
                <Section title="Domains" actionText="view all" onClickAction={navigateToDomains}>
                    <Carousel>
                        {domains.map((domain) => (
                            <DomainCard key={domain.urn} domain={domain} />
                        ))}
                    </Carousel>
                </Section>
            )) ||
                null}
        </div>
    );
};
