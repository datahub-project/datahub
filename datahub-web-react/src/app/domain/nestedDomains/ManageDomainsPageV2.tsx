import { PlusOutlined } from '@ant-design/icons';
import { useApolloClient } from '@apollo/client';
import { Button } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import CreateDomainModal from '@app/domain/CreateDomainModal';
import { useDomainsContext } from '@app/domain/DomainsContext';
import DomainsTitle from '@app/domain/nestedDomains/DomainsTitle';
import RootDomains from '@app/domain/nestedDomains/RootDomains';
import { updateListDomainsCache } from '@app/domain/utils';
import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { DOMAINS_CREATE_DOMAIN_ID, DOMAINS_INTRO_ID } from '@app/onboarding/config/DomainsOnboardingConfig';

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
