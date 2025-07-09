import React from 'react';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { Announcements } from '@app/homeV3/announcements/Announcements';
import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';
import { CenteredContainer, ContentContainer, ContentDiv } from '@app/homeV3/styledComponents';
import Template from '@app/homeV3/template/Template';

const HomePageContent = () => {
    const { settings, loaded: globalSettingsLoaded } = useGlobalSettings();
    const { user, loaded: userLoaded } = useUserContext();

    const personalTemplate = user?.settings?.homePage?.pageTemplate || null;
    const globalTemplate = settings.globalHomePageSettings?.defaultTemplate || null;

    if (!userLoaded || !globalSettingsLoaded) return null;

    return (
        <PageTemplateProvider personalTemplate={personalTemplate} globalTemplate={globalTemplate}>
            <ContentContainer>
                <CenteredContainer>
                    <ContentDiv>
                        <Announcements />
                        <Template />
                    </ContentDiv>
                </CenteredContainer>
            </ContentContainer>
        </PageTemplateProvider>
    );
};

export default HomePageContent;
