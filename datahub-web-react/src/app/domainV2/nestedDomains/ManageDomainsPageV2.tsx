import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { Button } from '@src/alchemy-components';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';
import { useApolloClient } from '@apollo/client';
import RootDomains from './RootDomains';
import { DOMAINS_CREATE_DOMAIN_ID, DOMAINS_INTRO_ID } from '../../onboarding/config/DomainsOnboardingConfig';
import { OnboardingTour } from '../../onboarding/OnboardingTour';
import CreateDomainModal from '../CreateDomainModal';
import { updateListDomainsCache } from '../utils';
import { useDomainsContext as useDomainsContextV2 } from '../DomainsContext';

const PageWrapper = styled.div`
    background-color: #ffffff;
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    border-radius: 8px;
    margin-left: 12px;
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

    useEffect(() => {
        setEntityData(null);
    }, [setEntityData]);

    return (
        <PageWrapper>
            <OnboardingTour stepIds={[DOMAINS_INTRO_ID, DOMAINS_CREATE_DOMAIN_ID]} />
            <Header>
                <PageTitle title="Domains" subTitle="Group your data assets using hierarchical collections" />
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
