import React from 'react';

import { Announcements } from '@app/homeV3/announcements/Announcements';
import Module from '@app/homeV3/module/Module';
import { ModuleProps } from '@app/homeV3/module/types';
import { CenteredContainer, ContentContainer, ContentDiv } from '@app/homeV3/styledComponents';

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
        <ContentContainer>
            <CenteredContainer>
                <ContentDiv>
                    <Announcements />
                    {SAMPLE_MODULES.map((sampleModule) => (
                        <Module {...sampleModule} key={sampleModule.name} />
                    ))}
                </ContentDiv>
            </CenteredContainer>
        </ContentContainer>
    );
};

export default HomePageContent;
