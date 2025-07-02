import React from 'react';

import { Announcements } from '@app/homeV3/announcements/Announcements';
import { ContentContainer, ContentWrapper, StyledVectorBackground } from '@app/homeV3/styledComponents';

const HomePageContent = () => {
    return (
        <ContentWrapper>
            <StyledVectorBackground />
            <ContentContainer>
                <Announcements />
            </ContentContainer>
        </ContentWrapper>
    );
};

export default HomePageContent;
