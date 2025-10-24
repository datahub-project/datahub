import { PageTitle } from '@components';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import HelpLinkExample from '@app/settingsV2/helpLink/HelpLinkExample';
import HelpLinkForm from '@app/settingsV2/helpLink/HelpLinkForm';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
    gap: 16px;
`;

const PageContentWrapper = styled.div`
    display: flex;
`;

export default function ManageHelpLink() {
    const { helpLinkState } = useGlobalSettingsContext();
    const { resetHelpLinkState } = helpLinkState;

    useEffect(() => {
        return () => resetHelpLinkState();
    }, [resetHelpLinkState]);

    return (
        <PageContainer>
            <PageTitle
                title="Custom Help Link"
                subTitle="Configure a custom help link that appears in the help menu."
            />
            <PageContentWrapper>
                <HelpLinkForm />
                <HelpLinkExample />
            </PageContentWrapper>
        </PageContainer>
    );
}
