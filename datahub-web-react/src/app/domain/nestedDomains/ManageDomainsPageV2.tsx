import { Button } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components/macro';
import DomainsTitle from './DomainsTitle';
import RootDomains from './RootDomains';
import { DOMAINS_CREATE_DOMAIN_ID, DOMAINS_INTRO_ID } from '../../onboarding/config/DomainsOnboardingConfig';
import { OnboardingTour } from '../../onboarding/OnboardingTour';

const PageWrapper = styled.div`
    background-color: #f8f9fa;
    flex: 1;
    display: flex;
    flex-direction: column;
`;

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    padding: 32px 24px;
    font-size: 30px;
    align-items: center;
`;

export default function ManageDomainsPageV2() {
    return (
        <PageWrapper>
            <OnboardingTour stepIds={[DOMAINS_INTRO_ID, DOMAINS_CREATE_DOMAIN_ID]} />
            <Header>
                <DomainsTitle />
                {/* TODO: Add functionality to this button */}
                <Button type="primary" id={DOMAINS_CREATE_DOMAIN_ID}>
                    <PlusOutlined /> New Domain
                </Button>
            </Header>
            <RootDomains />
        </PageWrapper>
    );
}
