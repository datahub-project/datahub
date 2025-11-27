import React from 'react';

import { Announcements } from '@app/homeV3/announcements/Announcements';
import FreeTrialContent from '@app/homeV3/FreeTrialContent';
import EditDefaultTemplateBar from '@app/homeV3/settings/EditDefaultTemplateBar';
import HomePageSettingsButtonWrapper from '@app/homeV3/settings/HomePageSettingsButtonWrapper';
import { CenteredContainer, ContentContainer, ContentDiv } from '@app/homeV3/styledComponents';
import Template from '@app/homeV3/template/Template';
import { useIsFreeTrialInstance } from '@app/useAppConfig';

const HomePageContent = () => {
    const isFreeTrialInstance = useIsFreeTrialInstance();
    return (
        <ContentContainer>
            <CenteredContainer>
                <ContentDiv>
                    <HomePageSettingsButtonWrapper />
                    <Announcements />
                    {isFreeTrialInstance ? <FreeTrialContent /> : <Template />}
                    <EditDefaultTemplateBar />
                </ContentDiv>
            </CenteredContainer>
        </ContentContainer>
    );
};

export default HomePageContent;
