import React from 'react';

import Module from '@app/homepageV2/module/Module';
import { ModuleProps } from '@app/homepageV2/module/types';
import { ContentContainer, ContentWrapper, StyledVectorBackground } from '@app/homepageV2/styledComponents';

const SAMPLE_MODULES: ModuleProps[] = [
    {
        name: 'Your Assets',
        description: 'These are assets you are the owner',
        type: 'yourAssets',
        visibility: 'personal',
    },
    {
        name: 'Sample large module',
        description: 'Description of the sample module',
        type: 'sampleLarge',
        visibility: 'global',
    },
];

const HomePageContent = () => {
    return (
        <ContentWrapper>
            <StyledVectorBackground />
            <ContentContainer>
                {SAMPLE_MODULES.map((sampleModule) => (
                    <Module {...sampleModule} key={sampleModule.name} />
                ))}
            </ContentContainer>
        </ContentWrapper>
    );
};

export default HomePageContent;
