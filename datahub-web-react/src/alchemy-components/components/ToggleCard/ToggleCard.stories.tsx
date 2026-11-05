import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { fn } from '@storybook/test';
import React, { useEffect, useState } from 'react';

import { Text } from '@components/components/Text';
import { ToggleCard } from '@components/components/ToggleCard';
import { ToggleCardProps } from '@components/components/ToggleCard/types';

const meta = {
    title: 'Components / ToggleCard',
    component: ToggleCard,

    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A card with a title, optional subtitle and a switch for turning a setting on or off.',
        },
    },

    argTypes: {
        title: {
            description: 'Title of the setting.',
            control: { type: 'text' },
        },
        subTitle: {
            description: 'Text or node shown below the title.',
            control: { type: 'text' },
        },
        value: {
            description: 'Whether the switch is on.',
            control: { type: 'boolean' },
        },
        disabled: {
            description: 'Disables the switch.',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'false' } },
        },
        onToggle: {
            description: 'Called with the new value when the switch is toggled.',
            control: false,
        },
        children: {
            description: 'Content rendered below the header.',
            control: false,
        },
    },

    args: {
        title: 'Email notifications',
        subTitle: 'Get an email when an asset you own changes.',
        value: false,
        onToggle: fn(),
    },
} satisfies Meta<typeof ToggleCard>;

export default meta;

type Story = StoryObj<typeof meta>;

// ToggleCard is controlled, so the stories keep the switch value in local state.
const ControlledToggleCard = ({ value, onToggle, ...props }: ToggleCardProps) => {
    const [checked, setChecked] = useState(value);
    useEffect(() => setChecked(value), [value]);
    return (
        <div style={{ width: 400 }}>
            <ToggleCard
                {...props}
                value={checked}
                onToggle={(next) => {
                    setChecked(next);
                    onToggle(next);
                }}
            />
        </div>
    );
};

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <ControlledToggleCard {...props} />,
};

export const basic: Story = {
    render: (props) => <ControlledToggleCard {...props} />,
};

export const disabled: Story = {
    args: {
        value: true,
        disabled: true,
    },
    render: (props) => <ControlledToggleCard {...props} />,
};

export const withChildren: Story = {
    args: {
        value: true,
        children: <Text color="gray">Sent at most once a day.</Text>,
    },
    render: (props) => <ControlledToggleCard {...props} />,
};
