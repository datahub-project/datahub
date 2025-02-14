import { Button, Text } from '@components';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { Modal } from '.';
import { ModalButton } from './Modal';

type ModalWithExampleButtons = React.ComponentProps<typeof Modal> & { buttonExamples?: string[]; content?: string };

const meta = {
    title: 'Components / Modal',
    component: Modal,

    // Display Properties
    parameters: {
        badges: [BADGE.BETA],
        docs: {
            subtitle: 'Single component for all modals, backed by Ant modal',
        },
    },

    // Component-level argTypes
    argTypes: {
        buttons: {
            description: "Buttons to display in the modal's footer",
            control: 'object',
        },
        buttonExamples: {
            options: ['Cancel', 'Propose', 'Submit'],
            description: 'Example buttons for storybook',
            control: 'check',
        },
        title: {
            description: 'The title of the modal',
            control: 'text',
        },
        subtitle: {
            description: 'Secondary title that goes below the title in the header',
            control: 'text',
        },
        content: {
            description: 'Example content of the modal',
            control: 'text',
        },
        onCancel: {
            description: 'Function to call when the modal is closed, mainly to hide the modal',
        },
    },

    // Define defaults
    args: {
        buttons: [],
        buttonExamples: [],
        title: 'Title',
        subtitle: 'This is the modal subtitle',
        content: '',
        onCancel: () => {},
    },
} satisfies Meta<ModalWithExampleButtons>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: Render,
};

// eslint-disable-next-line react/prop-types
function Render({ buttons, buttonExamples, content, ...props }: ModalWithExampleButtons) {
    const [isOpen, setIsOpen] = React.useState(false);

    const exampleButtons: ModalButton[] =
        buttonExamples?.map((text) => {
            switch (text) {
                case 'Cancel':
                    return { text, variant: 'text', onClick: () => setIsOpen(false) };
                case 'Propose':
                    // TODO: Replace with secondary variant once it's supported
                    return { text, variant: 'outline', onClick: () => setIsOpen(false) };
                case 'Submit':
                default:
                    return { text, variant: 'filled', onClick: () => setIsOpen(false) };
            }
        }) || [];

    return (
        <>
            <Button onClick={() => setIsOpen(true)}>Open Modal</Button>
            {isOpen && (
                <Modal {...props} buttons={[...buttons, ...exampleButtons]} onCancel={() => setIsOpen(false)}>
                    {content && <Text color="gray">{content}</Text>}
                </Modal>
            )}
        </>
    );
}
