import React from 'react';

import Module from '@app/homepageV2/module/Module';
import { ContentContainer, ContentWrapper, StyledVectorBackground } from '@app/homepageV2/styledComponents';

const HomePageContent = () => {
    return (
        <ContentWrapper>
            <StyledVectorBackground />
            <ContentContainer>
                <Module
                    name="Sample large module"
                    description="Description of the sample module"
                    type="sampleLarge"
                    visibility="global"
                />
            </ContentContainer>
        </ContentWrapper>
    );
};

export default HomePageContent;
