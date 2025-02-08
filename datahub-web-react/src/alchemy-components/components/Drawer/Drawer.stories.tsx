import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';
import { Button } from '../Button';
import { Drawer } from './Drawer';
import { DrawerProps } from './types';
import { drawerDefault } from './defaults';

// Auto Docs
const meta = {
    title: 'Components / Drawer',
    component: Drawer,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A panel which slides in from the edge of the screen.',
        },
    },

    // Component-level argTypes
    argTypes: {
        title: {
            description: 'The title of the drawer',
            control: {
                type: 'text',
            },
        },
        open: {
            description: 'is the drawer opened',
            control: {
                type: 'boolean',
            },
        },
        onClose: {
            description: 'The handler called when the drawer is closed',
        },
        width: {
            description: 'Width of the drawer',
            table: {
                defaultValue: { summary: `${drawerDefault.width}` },
            },
            control: {
                type: 'number',
            },
        },
        closable: {
            description: 'Whether a close (x) button is visible on top left of the Drawer dialog or not',
            table: {
                defaultValue: { summary: `${drawerDefault.closable}` },
            },
            control: {
                type: 'boolean',
            },
        },
        maskTransparent: {
            description: 'Whether the mask is visible',
            table: {
                defaultValue: { summary: `${drawerDefault.maskTransparent}` },
            },
            control: {
                type: 'boolean',
            },
        },
    },

    // Define default args
    args: {
        title: 'Title',
        ...drawerDefault,
    },
} satisfies Meta<typeof Drawer>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

const WrappedDrawer = ({ ...props }: DrawerProps) => {
    const [isOpen, setIsOpen] = useState<boolean>(false);
    return (
        <>
            <Button onClick={() => setIsOpen(true)}>Open</Button>
            <Drawer {...props} open={isOpen} onClose={() => setIsOpen(false)}>
                Content
            </Drawer>
        </>
    );
};
// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <WrappedDrawer {...props} />,
};
