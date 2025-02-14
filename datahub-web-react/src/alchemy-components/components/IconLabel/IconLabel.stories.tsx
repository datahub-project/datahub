import React from 'react';
import { Meta, StoryObj } from '@storybook/react';
import { Airplane } from '@phosphor-icons/react';

import { IconLabel } from './IconLabel';
import { IconType } from './types';

const meta: Meta<typeof IconLabel> = {
    title: 'Components / IconLabel',
    component: IconLabel,

    // Component-level parameters
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'Displays a label with an icon or image.',
        },
    },

    // Component-level argTypes
    argTypes: {
        icon: {
            description: 'The icon or image source to display.',
            control: false,
            table: {
                type: { summary: 'ReactNode' },
            },
        },
        name: {
            description: 'The label text.',
            control: 'text',
            table: {
                type: { summary: 'string' },
            },
        },
        imageUrl: {
            description: 'The image url.',
            control: 'text',
            table: {
                type: { summary: 'string' },
            },
        },
        type: {
            description: 'The type of icon to render.',
            control: 'radio',
            options: [IconType.ICON, IconType.IMAGE],
            table: {
                defaultValue: { summary: IconType.ICON },
                type: { summary: 'IconType.ICON | IconType.IMAGE' },
            },
        },
    },

    // Default props
    args: {
        icon: <Airplane size={20} />, // Default to an emoji for demonstration
        name: 'Icon Label',
        type: IconType.ICON,
    },
};

export default meta;

type Story = StoryObj<typeof meta>;

// Sandbox Story
export const sandbox: Story = {
    render: (props) => <IconLabel {...props} />,
};

// Example Stories
export const withIcon: Story = {
    args: {
        icon: <Airplane size={20} />,
        name: 'Airplane Icon',
        type: IconType.ICON,
    },
};

export const withImage: Story = {
    args: {
        imageUrl: 'https://png.pngtree.com/png-vector/20230209/ourmid/pngtree-test-icon-png-image_6591706.png',
        name: 'Placeholder Image',
        type: IconType.IMAGE,
        icon: <Airplane size={20} />,
    },
};

export const longTextLabel: Story = {
    args: {
        icon: <Airplane size={20} />,
        name: 'This is a very long label text for testing purposes',
        type: IconType.ICON,
    },
};
