/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import ClickOutside from '@components/components/Utils/ClickOutside/ClickOutside';

// Auto Docs
const meta = {
    title: 'Utils / ClickOutside',
    component: ClickOutside,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'This component allows to add autocompletion',
        },
    },

    // Component-level argTypes
    argTypes: {
        onClickOutside: {
            description: 'Called on clicking outside',
        },
        ignoreSelector: {
            description: 'Optional CSS-selector to ignore handling of clicks as outside clicks',
        },
        outsideSelector: {
            description: 'Optional CSS-selector to consider clicked element as outside click',
        },
        ignoreWrapper: {
            description: 'Enable to ignore clicking outside of wrapper',
        },
        width: {
            description: 'Customize the width of the wrapper',
            table: {
                defaultValue: {
                    summary: 'fit-content',
                },
            },
        },
    },

    // Define defaults
    args: {
        onClickOutside: () => console.log('Clicked outside'),
    },
} satisfies Meta<typeof ClickOutside>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <ClickOutside {...props}>
            <button type="button">Button</button>
        </ClickOutside>
    ),
};
