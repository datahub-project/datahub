import React from 'react';
import { Story, Meta } from '@storybook/react';

import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import Lineage, { Props } from '../Lineage';
import { sampleRelationship } from './lineageEntities';

export default {
    title: 'Dataset Profile / Lineage',
    component: Lineage,
} as Meta;

const Template: Story<Props> = (args) => <Lineage {...args} />;

export const UpstreamAndDownstream = Template.bind({});
UpstreamAndDownstream.args = { upstreamLineage: sampleRelationship, downstreamLineage: sampleRelationship };
UpstreamAndDownstream.decorators = [
    (InnerStory) => (
        <TestPageContainer>
            <InnerStory />
        </TestPageContainer>
    ),
];
