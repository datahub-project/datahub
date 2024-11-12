import React, { useEffect, useState } from 'react';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { Button } from '@src/alchemy-components';
import styled from 'styled-components/macro';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';
import { useApolloClient } from '@apollo/client';
import RootDomains from './RootDomains';
import { DOMAINS_CREATE_DOMAIN_ID, DOMAINS_INTRO_ID } from '../../onboarding/config/DomainsOnboardingConfig';
import { OnboardingTour } from '../../onboarding/OnboardingTour';
import CreateDomainModal from '../CreateDomainModal';
import { updateListDomainsCache } from '../utils';
import { useDomainsContext as useDomainsContextV2 } from '../DomainsContext';

const PageWrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: #ffffff;
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    margin-left: ${(props) => (props.$isShowNavBarRedesign ? '0' : '12px')};
    ${(props) => props.$isShowNavBarRedesign && `box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};`}
`;

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    padding: 16px 20px 20px 20px;
    align-items: center;
`;

export default function ManageDomainsPageV2() {
    const { setEntityData } = useDomainsContextV2();
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);
    const client = useApolloClient();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    useEffect(() => {
        setEntityData(null);
    }, [setEntityData]);

    return (
        <PageWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
            <OnboardingTour stepIds={[DOMAINS_INTRO_ID, DOMAINS_CREATE_DOMAIN_ID]} />
            <Header>
                <PageTitle title="Domains" subTitle="Group data assets using hierarchical collections" />
                <Button
                    id={DOMAINS_CREATE_DOMAIN_ID}
                    onClick={() => setIsCreatingDomain(true)}
                    data-testid="domains-new-domain-button"
                    icon="Add"
                >
                    Create
                </Button>
            </Header>
            <RootDomains setIsCreatingDomain={setIsCreatingDomain} />
            {isCreatingDomain && (
                <CreateDomainModal
                    onClose={() => setIsCreatingDomain(false)}
                    onCreate={(urn, id, name, description, parentDomain) =>
                        updateListDomainsCache(client, urn, id, name, description, parentDomain)
                    }
                />
            )}
        </PageWrapper>
    );
}
