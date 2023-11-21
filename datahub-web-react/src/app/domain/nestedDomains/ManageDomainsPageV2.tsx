import { useApolloClient } from '@apollo/client';
import { Button } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import DomainsTitle from './DomainsTitle';
import RootDomains from './RootDomains';
import { DOMAINS_CREATE_DOMAIN_ID, DOMAINS_INTRO_ID } from '../../onboarding/config/DomainsOnboardingConfig';
import { OnboardingTour } from '../../onboarding/OnboardingTour';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';
import CreateDomainModal from '../CreateDomainModal';
import { updateListDomainsCache } from '../utils';
import { useDomainsContext } from '../DomainsContext';

const PageWrapper = styled.div`
    background-color: ${ANTD_GRAY_V2[1]};
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
`;

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    padding: 32px 24px;
    font-size: 30px;
    align-items: center;
`;

export default function ManageDomainsPageV2() {
    const { setEntityData, setParentDomainsToUpdate } = useDomainsContext();
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);
    const client = useApolloClient();

    useEffect(() => {
        setEntityData(null);
    }, [setEntityData]);

    return (
        <PageWrapper>
            <OnboardingTour stepIds={[DOMAINS_INTRO_ID, DOMAINS_CREATE_DOMAIN_ID]} />
            <Header>
                <DomainsTitle />
                <Button
                    type="primary"
                    id={DOMAINS_CREATE_DOMAIN_ID}
                    onClick={() => setIsCreatingDomain(true)}
                    data-testid="domains-new-domain-button"
                >
                    <PlusOutlined /> New Domain
                </Button>
            </Header>
            <RootDomains setIsCreatingDomain={setIsCreatingDomain} />
            {isCreatingDomain && (
                <CreateDomainModal
                    onClose={() => setIsCreatingDomain(false)}
                    onCreate={(urn, id, name, description, parentDomain) => {
                        updateListDomainsCache(client, urn, id, name, description, parentDomain);
                        if (parentDomain) setParentDomainsToUpdate([parentDomain]);
                    }}
                />
            )}
        </PageWrapper>
    );
}
