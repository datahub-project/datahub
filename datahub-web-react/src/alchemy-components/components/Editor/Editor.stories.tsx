import type { Meta, StoryObj } from '@storybook/react';
import { BoldExtension, ItalicExtension } from 'remirror/extensions';
import { Remirror, ThemeProvider, useRemirror } from '@remirror/react';
import React from 'react';
import UserContextProvider from '@src/app/context/UserContextProvider';
import { MockedProvider } from '@apollo/client/testing';
import { Editor } from './Editor';
import { EditorTheme } from './EditorTheme';

const meta = {
    title: 'Components / Editor',
    component: Editor,
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'A rich text editor with markdown support.',
        },
    },
    decorators: [
        (Story) => {
            return (
                <MockedProvider mocks={[]} addTypename={false}>
                    <ThemeProvider theme={EditorTheme}>
                        <UserContextProvider>
                            <Story />
                        </UserContextProvider>
                    </ThemeProvider>
                </MockedProvider>
            );
        },
    ],
    argTypes: {
        readOnly: { description: 'Sets the editor to read-only mode.', control: { type: 'boolean' } },
        content: { description: 'Initial content of the editor.', control: { type: 'text' } },
        onChange: { description: 'Callback triggered when content changes.', action: 'contentChanged' },
        placeholder: { description: 'Placeholder text.', control: { type: 'text' } },
        className: { description: 'CSS class for editor container.', control: { type: 'text' } },
    },
    args: {
        readOnly: false,
        content: '',
        placeholder: 'Type here...',
        className: '',
    },
} satisfies Meta<typeof Editor>;

export default meta;

type Story = StoryObj<typeof meta>;

/**
 * A wrapper component to handle Remirror setup for the stories.
 */
const RemirrorWrapper = ({ children, content }: { children: React.ReactNode; content?: string }) => {
    const { manager, state } = useRemirror({
        extensions: () => [new BoldExtension({}), new ItalicExtension()],
        stringHandler: 'markdown',
        content,
    });

    return (
        <Remirror manager={manager} initialContent={state}>
            {children}
        </Remirror>
    );
};

// export const ReadOnlyEditor: Story = {
//     tags: ['dev'],
//     render: (props) => (
//         <RemirrorWrapper content="# Read-Only Editor\nThis is demo content.">
//             <Editor {...props} readOnly />
//         </RemirrorWrapper>
//     ),
// };

export const EditableEditor: Story = {
    tags: ['dev'],
    render: (props) => (
        <RemirrorWrapper>
            <Editor {...props} placeholder="Type your content here..." />
        </RemirrorWrapper>
    ),
};

// export const EditorWithPreloadedContent: Story = {
//     tags: ['dev'],
//     render: (props) => (
//         <RemirrorWrapper content="# Preloaded Content\nThis editor has some preloaded content.">
//             <Editor {...props} />
//         </RemirrorWrapper>
//     ),
// };
