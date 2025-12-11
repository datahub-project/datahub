/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

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
        variant: {
            description: 'The variant of header based on its usage',
        },
    },

    // Define default args
    args: {
        title: 'Automations',
        subTitle: 'Create & manage automations',
        variant: 'pageHeader',
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

export const sectionHeader = () => (
    <PageTitle title="Section header title" subTitle="Section header subtitle text" variant="sectionHeader" />
);
