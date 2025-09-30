import React from 'react';

import { Announcements } from '@app/homeV3/announcements/Announcements';
import EditDefaultTemplateBar from '@app/homeV3/settings/EditDefaultTemplateBar';
// import EditHomePageSettingsButton from '@app/homeV3/settings/EditHomePageSettingsButton';
import { CenteredContainer, ContentContainer, ContentDiv } from '@app/homeV3/styledComponents';
import Template from '@app/homeV3/template/Template';

const HomePageContent = () => {
    return (
        <ContentContainer>
            <CenteredContainer>
                <ContentDiv>
                    {/* <EditHomePageSettingsButton /> Editing templates is not enabled in OSS */}
                    <Announcements />
                    <Template />
                    <EditDefaultTemplateBar />
                </ContentDiv>
            </CenteredContainer>
        </ContentContainer>
    );
};

export default HomePageContent;
