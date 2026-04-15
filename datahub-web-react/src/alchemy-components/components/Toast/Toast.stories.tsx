import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { GridList } from '@components/.docs/mdx-components';
import { Button } from '@components/components/Button';
import { ToastRenderer, toast } from '@components/components/Toast';
import { ToastVariant } from '@components/components/Toast/types';

import themes from '@conf/theme/themes';

const meta: Meta = {
    title: 'Components / Toast',
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'Imperative toast notifications with semantic variants and actions',
        },
    },
    decorators: [
        (Story) => (
            <ThemeProvider theme={themes.themeV2}>
                <ToastRenderer />
                <Story />
            </ThemeProvider>
        ),
    ],
};

export default meta;

interface SandboxArgs {
    variant: ToastVariant;
    message: string;
    duration: number;
    showUndo: boolean;
}

export const sandbox: StoryObj<SandboxArgs> = {
    tags: ['dev'],
    argTypes: {
        variant: {
            description: 'Toast variant',
            control: { type: 'select' },
            options: ['success', 'error', 'warning', 'info', 'loading'],
        },
        message: {
            description: 'Toast message content',
            control: { type: 'text' },
        },
        duration: {
            description: 'Duration in seconds (0 = persistent)',
            control: { type: 'number', min: 0, max: 30 },
        },
        showUndo: {
            description: 'Show undo action button',
            control: { type: 'boolean' },
        },
    },
    args: {
        variant: 'success',
        message: 'Action completed successfully',
        duration: 3,
        showUndo: false,
    },
    render: ({ variant, message, duration, showUndo }) => (
        <Button
            onClick={() =>
                toast[variant](message, {
                    duration,
                    actions: showUndo ? [{ label: 'Undo', onClick: () => toast.info('Undone!') }] : undefined,
                })
            }
        >
            Show Toast
        </Button>
    ),
};

export const allVariants = () => (
    <GridList>
        <Button onClick={() => toast.success('Changes saved')}>Success</Button>
        <Button onClick={() => toast.error('Failed to save changes')}>Error</Button>
        <Button onClick={() => toast.warning('Unsaved changes will be lost')}>Warning</Button>
        <Button onClick={() => toast.info('3 items selected')}>Info</Button>
        <Button onClick={() => toast.loading('Uploading...', { duration: 3 })}>Loading</Button>
    </GridList>
);

export const withUndoAction = () => (
    <GridList>
        <Button
            onClick={() =>
                toast.success('500 items updated', {
                    actions: [{ label: 'Undo', onClick: () => toast.info('Undone!') }],
                })
            }
        >
            Success with Undo
        </Button>
        <Button
            onClick={() =>
                toast.error('3 items deleted', {
                    actions: [{ label: 'Undo', onClick: () => toast.info('Restored!') }],
                })
            }
        >
            Error with Undo
        </Button>
        <Button
            onClick={() =>
                toast.warning('Permission revoked', {
                    actions: [{ label: 'Undo', onClick: () => toast.info('Permission restored') }],
                })
            }
        >
            Warning with Undo
        </Button>
    </GridList>
);

export const persistent = () => (
    <GridList>
        <Button
            onClick={() =>
                toast.error('Connection lost', {
                    duration: 0,
                    key: 'persistent-demo',
                })
            }
        >
            Show Persistent Toast
        </Button>
        <Button onClick={() => toast.destroy('persistent-demo')}>Dismiss It</Button>
    </GridList>
);

export const stacking = () => (
    <Button
        onClick={() => {
            toast.success('First notification');
            setTimeout(() => toast.info('Second notification'), 300);
            setTimeout(() => toast.warning('Third notification'), 600);
        }}
    >
        Show 3 Stacked Toasts
    </Button>
);
