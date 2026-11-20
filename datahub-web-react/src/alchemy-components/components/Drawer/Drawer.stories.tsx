import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { Drawer } from '@components/components/Drawer/Drawer';
import { drawerDefault } from '@components/components/Drawer/defaults';
import { DrawerProps } from '@components/components/Drawer/types';
import { Text } from '@components/components/Text';

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
            description: 'is the drawer opened. The stories toggle it with the Open button.',
            control: false,
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
            description: 'Whether a close (x) button is visible on top right of the Drawer dialog or not',
            table: {
                defaultValue: { summary: `${drawerDefault.closable}` },
            },
            control: {
                type: 'boolean',
            },
        },
        maskTransparent: {
            description: 'Whether the mask behind the drawer is transparent instead of dimmed',
            table: {
                defaultValue: { summary: `${drawerDefault.maskTransparent}` },
            },
            control: {
                type: 'boolean',
            },
        },
        header: {
            description: 'Custom header rendered in place of the default title bar (title, back and close buttons)',
            control: false,
        },
        showHeader: {
            description: 'Set to false to render no header at all, e.g. when the content renders its own header',
            table: {
                defaultValue: { summary: `${drawerDefault.showHeader}` },
            },
            control: {
                type: 'boolean',
            },
        },
        footer: {
            description: 'Content rendered in a footer pinned below the scrollable body',
            control: false,
        },
        mask: {
            description: 'Whether to render the mask behind the drawer',
            table: {
                defaultValue: { summary: `${drawerDefault.mask}` },
            },
            control: {
                type: 'boolean',
            },
        },
        maskClosable: {
            description: 'Whether clicking the mask closes the drawer',
            table: {
                defaultValue: { summary: `${drawerDefault.maskClosable}` },
            },
            control: {
                type: 'boolean',
            },
        },
        placement: {
            description: 'The edge of the screen (or container) the drawer slides in from',
            table: {
                defaultValue: { summary: `${drawerDefault.placement}` },
            },
            options: ['top', 'right', 'bottom', 'left'],
            control: {
                type: 'select',
            },
        },
        getContainer: {
            description:
                'Where to mount the drawer. Pass an element, a getter or a selector to render it inline inside that element, or false to render in place',
            control: false,
        },
        push: {
            description: 'Whether a parent drawer is pushed aside when this drawer opens nested inside it',
            control: {
                type: 'boolean',
            },
        },
        autoFocus: {
            description: 'Whether the drawer takes focus when it opens',
            control: {
                type: 'boolean',
            },
        },
        bodyPadding: {
            description: 'Padding of the drawer body. Numbers are treated as pixels',
            table: {
                defaultValue: { summary: `${drawerDefault.bodyPadding}` },
            },
            control: {
                type: 'text',
            },
        },
        minWidth: {
            description: 'Minimum width of the drawer panel',
            control: {
                type: 'text',
            },
        },
        maxWidth: {
            description: 'Maximum width of the drawer panel',
            control: {
                type: 'text',
            },
        },
        dataTestId: {
            description: 'Prefix for the test id of the default title bar',
            control: {
                type: 'text',
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

const CONTAINER_ID = 'drawer-story-container';

// The transform makes this element the containing block for the drawer's fixed-position
// wrapper, so the drawer stays inside the container instead of covering the viewport.
const InlineContainer = styled.div`
    position: relative;
    transform: translateZ(0);
    overflow: hidden;
    width: 800px;
    height: 400px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    padding: 16px;
`;

const CustomHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 16px;
    background: ${(props) => props.theme.colors.bgSurface};
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const Footer = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

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

export const customHeader: Story = {
    render: (props) => {
        const CustomHeaderDrawer = () => {
            const [isOpen, setIsOpen] = useState<boolean>(false);
            return (
                <>
                    <Button onClick={() => setIsOpen(true)}>Open</Button>
                    <Drawer
                        {...props}
                        open={isOpen}
                        onClose={() => setIsOpen(false)}
                        bodyPadding={0}
                        header={
                            <CustomHeader>
                                <Text weight="bold" size="lg">
                                    Custom header
                                </Text>
                                <Button variant="text" onClick={() => setIsOpen(false)}>
                                    Close
                                </Button>
                            </CustomHeader>
                        }
                    >
                        Content without body padding
                    </Drawer>
                </>
            );
        };
        return <CustomHeaderDrawer />;
    },
};

export const withFooter: Story = {
    render: (props) => {
        const FooterDrawer = () => {
            const [isOpen, setIsOpen] = useState<boolean>(false);
            const close = () => setIsOpen(false);
            return (
                <>
                    <Button onClick={() => setIsOpen(true)}>Open</Button>
                    <Drawer
                        {...props}
                        open={isOpen}
                        onClose={close}
                        footer={
                            <Footer>
                                <Button variant="text" onClick={close}>
                                    Cancel
                                </Button>
                                <Button onClick={close}>Save</Button>
                            </Footer>
                        }
                    >
                        Content
                    </Drawer>
                </>
            );
        };
        return <FooterDrawer />;
    },
};

export const withoutHeader: Story = {
    args: {
        showHeader: false,
    },
    render: (props) => <WrappedDrawer {...props} />,
};

export const inlineContainer: Story = {
    args: {
        width: 320,
        mask: false,
        maskClosable: false,
        autoFocus: false,
    },
    render: (props) => {
        const InlineDrawer = () => {
            const [isOpen, setIsOpen] = useState<boolean>(false);
            return (
                <InlineContainer id={CONTAINER_ID}>
                    <Button onClick={() => setIsOpen((value) => !value)}>Toggle</Button>
                    <Drawer
                        {...props}
                        open={isOpen}
                        onClose={() => setIsOpen(false)}
                        getContainer={() => document.getElementById(CONTAINER_ID) as HTMLElement}
                    >
                        This drawer is mounted inside the bordered container instead of the page body, so the rest of
                        the container stays interactive.
                    </Drawer>
                </InlineContainer>
            );
        };
        return <InlineDrawer />;
    },
};
