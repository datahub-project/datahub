import React, { useEffect } from 'react';
import styled from 'styled-components';
import { PageContainer, PageHeaderContainer, PageTitle } from '../posts/ManagePosts';
import { useGlobalSettingsContext } from '../../context/GlobalSettings/GlobalSettingsContext';
import HelpLinkForm from './HelpLinkForm';
import HelpLinkExample from './HelpLinkExample';

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
