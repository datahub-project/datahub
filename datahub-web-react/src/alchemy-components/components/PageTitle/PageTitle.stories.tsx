import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { PageTitle } from '.';

// Auto Docs
const meta = {
    title: 'Pages / Page Title',
    component: PageTitle,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Used to render the title and subtitle for a page.',
        },
    },

    // Component-level argTypes
    argTypes: {
        title: {
            description: 'The title text',
        },
        subTitle: {
            description: 'The subtitle text',
        },
    },

    // Define default args
    args: {
        title: 'Automations',
        subTitle: 'Create & manage automations',
    },
} satisfies Meta<typeof PageTitle>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <PageTitle {...props} />,
};

export const withLink = () => (
    <PageTitle
        title="Automations"
        subTitle={
            <>
                Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas aliquet nulla id felis vehicula, et
                posuere dui dapibus. <a href="/">Nullam rhoncus massa non tortor convallis</a>, in blandit turpis
                rutrum. Morbi tempus velit mauris, at mattis metus mattis sed. Nunc molestie efficitur lectus, vel
                mollis eros.
            </>
        }
    />
);
