import React from 'react';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { Announcements } from '@app/homeV3/announcements/Announcements';
import { CenteredContainer, ContentContainer, ContentDiv } from '@app/homeV3/styledComponents';
import Template from '@app/homeV3/template/Template';

const HomePageContent = () => {
    const { settings } = useGlobalSettings();
    const { user } = useUserContext();

    const template = user?.settings?.homePage?.pageTemplate || settings.globalHomePageSettings?.defaultTemplate;

    return (
        <ContentContainer>
            <CenteredContainer>
                <ContentDiv>
                    <Announcements />
                    <Template template={template} />
                </ContentDiv>
            </CenteredContainer>
        </ContentContainer>
    );
};

export default HomePageContent;
