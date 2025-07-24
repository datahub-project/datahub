import React from 'react';

import EditDefaultTemplateBar from '@app/homeV3/EditDefaultTemplateBar';
import EditDefaultTemplateButton from '@app/homeV3/EditDefaultTemplateButton';
import { Announcements } from '@app/homeV3/announcements/Announcements';
import { CenteredContainer, ContentContainer, ContentDiv } from '@app/homeV3/styledComponents';
import Template from '@app/homeV3/template/Template';

const HomePageContent = () => {
    return (
        <ContentContainer>
            <CenteredContainer>
                <ContentDiv>
                    <EditDefaultTemplateButton />
                    <Announcements />
                    <Template />
                    <EditDefaultTemplateBar />
                </ContentDiv>
            </CenteredContainer>
        </ContentContainer>
    );
};

export default HomePageContent;
