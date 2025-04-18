import React, { useEffect } from 'react';
import styled from 'styled-components';

import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import HelpLinkExample from '@app/settings/helpLink/HelpLinkExample';
import HelpLinkForm from '@app/settings/helpLink/HelpLinkForm';
import { PageContainer, PageHeaderContainer, PageTitle } from '@app/settings/posts/ManagePosts';

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
            <PageHeaderContainer>
                <PageTitle level={3}>Custom Help Link</PageTitle>
            </PageHeaderContainer>
            <PageContentWrapper>
                <HelpLinkForm />
                <HelpLinkExample />
            </PageContentWrapper>
        </PageContainer>
    );
}
