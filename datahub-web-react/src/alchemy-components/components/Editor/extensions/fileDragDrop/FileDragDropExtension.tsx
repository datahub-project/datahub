/* eslint-disable class-methods-use-this */
import {
    ApplySchemaAttributes,
    CommandFunction,
    ExtensionPriority,
    ExtensionTag,
    Handler,
    NodeExtension,
    NodeExtensionSpec,
    NodeSpecOverride,
    ProsemirrorAttributes,
    extension,
    isElementDomNode,
    omitExtraAttributes,
} from '@remirror/core';
import { NodeViewComponentProps } from '@remirror/react';
import { Plugin, PluginKey } from 'prosemirror-state';
import { EditorView } from 'prosemirror-view';
import React, { ComponentType } from 'react';

import { FileNodeView } from '@components/components/Editor/extensions/fileDragDrop/FileNodeView';

export const FILE_ATTRS = {
    url: 'data-file-url',
    name: 'data-file-name',
    type: 'data-file-type',
    size: 'data-file-size',
};

type FileNodeAttributes = ProsemirrorAttributes & {
    url: string;
    name: string;
    type: string;
    size: number;
};

interface FileDragDropOptions {
    onFileUpload?: (file: File) => Promise<string>; // Returns the uploaded file URL
    supportedTypes?: string[]; // MIME types to support
}

/**
 * The FileDragDrop extension allows users to drag and drop files into the editor.
 * It creates file nodes that render differently based on file type (images, PDFs, etc.)
 * and handles file uploads to S3 via pre-signed URLs.
 */
class FileDragDropExtension extends NodeExtension<FileDragDropOptions> {
    // constructor(options?: FileDragDropOptions) {
    //     super(options);
    //     console.log('FileDragDropExtension initialized with options:', options);
    // }

    get name() {
        return 'fileNode' as const;
    }

    createTags() {
        return [ExtensionTag.Block, ExtensionTag.Behavior, ExtensionTag.FormattingNode];
    }

    get defaultPriority() {
        return ExtensionPriority.High;
    }

    /**
     * Create the drag and drop plugin
     */
    createExternalPlugins(): Plugin[] {
        return [
            new Plugin({
                key: new PluginKey('fileDragDrop'),
                props: {
                    handleDOMEvents: {
                        drop: (view: EditorView, event: DragEvent) => {
                            console.log('🔥 FileDragDropExtension: Drop event detected!', event);
                            console.log('🔥 DataTransfer types:', event.dataTransfer?.types);
                            console.log('🔥 DataTransfer files:', event.dataTransfer?.files);
                            const result = this.handleDrop(view, event);
                            console.log('🔥 Drop handler result:', result);
                            return result;
                        },
                        dragover: (view: EditorView, event: DragEvent) => {
                            // Check if we have files
                            if (event.dataTransfer?.types.includes('Files')) {
                                console.log('🔥 Files detected in dragover - preventing default');
                                event.preventDefault();
                                event.dataTransfer.dropEffect = 'copy';
                                return true; // We handled this event
                            }
                            return false;
                        },
                        dragenter: (view: EditorView, event: DragEvent) => {
                            if (event.dataTransfer?.types.includes('Files')) {
                                console.log('🔥 Files detected in dragenter - preventing default');
                                event.preventDefault();
                                return true;
                            }
                            return false;
                        },
                        dragleave: (view: EditorView, event: DragEvent) => {
                            return false;
                        },
                    },
                },
            }),
        ];
    }

    private async handleDrop(view: EditorView, event: DragEvent): Promise<boolean> {
        console.log('handleDrop called with event:', event);
        event.preventDefault();
        event.stopPropagation();

        const { files } = event.dataTransfer || {};
        console.log('Files from dataTransfer:', files);

        if (!files || files.length === 0) {
            console.log('No files found in drop event');
            return false;
        }

        const supportedTypes = this.options.supportedTypes || [
            'image/jpeg',
            'image/png',
            'image/gif',
            'image/webp',
            'application/pdf',
            'text/plain',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        ];

        console.log('Supported types:', supportedTypes);

        // Get the position where the file was dropped
        const coordinates = view.posAtCoords({ left: event.clientX, top: event.clientY });
        console.log('Drop coordinates:', coordinates);

        if (!coordinates) {
            console.log('Could not get coordinates for drop position');
            // Fallback to current selection
            const { selection } = view.state;
            const pos = selection.from;
            console.log('Using fallback position:', pos);

            // Process each file at current selection
            for (let i = 0; i < files.length; i++) {
                const file = files[i];
                console.log('🔥 Processing file:', file.name, file.type, file.size);

                // Temporarily accept all file types for testing
                // if (!supportedTypes.includes(file.type)) {
                //     console.warn(`Unsupported file type: ${file.type}`);
                //     continue;
                // }

                try {
                    // Create a placeholder node first
                    const placeholderAttrs: FileNodeAttributes = {
                        url: '', // Will be filled after upload
                        name: file.name,
                        type: file.type,
                        size: file.size,
                    };

                    console.log('Creating node with attrs:', placeholderAttrs);
                    const node = this.type.create(placeholderAttrs);
                    const transaction = view.state.tr.insert(pos, node);
                    view.dispatch(transaction);

                    console.log('Node inserted, starting upload...');

                    // Upload the file if onFileUpload is provided
                    if (this.options.onFileUpload) {
                        try {
                            const uploadedUrl = await this.options.onFileUpload(file);
                            const finalUrl =
                                'https://file.notion.so/f/f/f818df0d-1067-44ab-99e1-8cf45d930c01/d79ab45e-aa89-41b1-aef7-4323d83b75eb/response_body_(2).txt?table=block&id=276fc6a6-4277-809b-ab50-ea9147dfed1a&spaceId=f818df0d-1067-44ab-99e1-8cf45d930c01&expirationTimestamp=1758672000000&signature=b0h8T76oI_6pBYBB73dUClYbLH1SNYpmcRqNJyD1iBU&downloadName=response_body+%2823%29.txt';
                            console.log('File uploaded, URL:', finalUrl);

                            // Find the node again in the current state (position might have changed)
                            const currentState = view.state;
                            const nodeAtPos = currentState.doc.nodeAt(pos);

                            if (nodeAtPos && nodeAtPos.type === this.type) {
                                // Update the node with the uploaded URL
                                const updatedAttrs = { ...placeholderAttrs, url: finalUrl };
                                const updatedNode = this.type.create(updatedAttrs);
                                const updateTransaction = currentState.tr.setNodeMarkup(pos, null, updatedAttrs);
                                view.dispatch(updateTransaction);
                                console.log('Node updated with uploaded URL');
                            } else {
                                console.log('Could not find node to update at position:', pos);
                            }
                        } catch (uploadError) {
                            console.error('Upload failed:', uploadError);
                        }
                    } else {
                        console.log('No upload handler provided');
                    }
                } catch (error) {
                    console.error('Error handling file drop:', error);
                }
            }
            return true;
        }

        // Process each file at drop coordinates
        for (let i = 0; i < files.length; i++) {
            const file = files[i];
            console.log('🔥 Processing file at coordinates:', file.name, file.type, file.size);

            // Temporarily accept all file types for testing
            // if (!supportedTypes.includes(file.type)) {
            //     console.warn(`Unsupported file type: ${file.type}`);
            //     continue;
            // }

            try {
                // Create a placeholder node first
                const placeholderAttrs: FileNodeAttributes = {
                    url: '', // Will be filled after upload
                    name: file.name,
                    type: file.type,
                    size: file.size,
                };

                console.log('Creating node with attrs:', placeholderAttrs);
                const node = this.type.create(placeholderAttrs);
                const transaction = view.state.tr.insert(coordinates.pos, node);
                view.dispatch(transaction);

                console.log('Node inserted, starting upload...');

                // Upload the file if onFileUpload is provided
                if (this.options.onFileUpload) {
                    try {
                        const uploadedUrl = await this.options.onFileUpload(file);
                        const finalUrl =
                            'https://file.notion.so/f/f/f818df0d-1067-44ab-99e1-8cf45d930c01/d79ab45e-aa89-41b1-aef7-4323d83b75eb/response_body_(2).txt?table=block&id=276fc6a6-4277-809b-ab50-ea9147dfed1a&spaceId=f818df0d-1067-44ab-99e1-8cf45d930c01&expirationTimestamp=1758672000000&signature=b0h8T76oI_6pBYBB73dUClYbLH1SNYpmcRqNJyD1iBU&downloadName=response_body+%2823%29.txt';
                        console.log('File uploaded, URL:', finalUrl);

                        // Find the node again in the current state (position might have changed)
                        const currentState = view.state;
                        const nodeAtPos = currentState.doc.nodeAt(coordinates.pos);

                        if (nodeAtPos && nodeAtPos.type === this.type) {
                            // Update the node with the uploaded URL
                            const updatedAttrs = { ...placeholderAttrs, url: finalUrl };
                            const updateTransaction = currentState.tr.setNodeMarkup(
                                coordinates.pos,
                                null,
                                updatedAttrs,
                            );
                            view.dispatch(updateTransaction);
                            console.log('Node updated with uploaded URL');
                        } else {
                            console.log('Could not find node to update at coordinates:', coordinates.pos);
                        }
                    } catch (uploadError) {
                        console.error('Upload failed:', uploadError);
                    }
                } else {
                    console.log('No upload handler provided');
                }
            } catch (error) {
                console.error('Error handling file drop:', error);
            }
        }

        return true;
    }

    createNodeSpec(extra: ApplySchemaAttributes, override: Partial<NodeSpecOverride>): NodeExtensionSpec {
        return {
            inline: false,
            group: 'block',
            marks: '',
            selectable: true,
            draggable: true,
            atom: true,
            ...override,
            attrs: {
                ...extra.defaults(),
                url: { default: '' },
                name: { default: '' },
                type: { default: '' },
                size: { default: 0 },
            },
            parseDOM: [
                {
                    tag: `div[${FILE_ATTRS.name}]`,
                    getAttrs: (node: string | Node) => {
                        if (!isElementDomNode(node)) {
                            return false;
                        }

                        const url = node.getAttribute(FILE_ATTRS.url) || '';
                        const name = node.getAttribute(FILE_ATTRS.name) || '';
                        const type = node.getAttribute(FILE_ATTRS.type) || '';
                        const size = parseInt(node.getAttribute(FILE_ATTRS.size) || '0', 10);

                        return { ...extra.parse(node), url, name, type, size };
                    },
                },
                ...(override.parseDOM ?? []),
            ],
            toDOM: (node) => {
                console.log('🔥 toDOM called for file node:', node);
                const { url, name, type, size } = omitExtraAttributes(node.attrs, extra) as FileNodeAttributes;

                const attrs = {
                    ...extra.dom(node),
                    class: 'file-node file-node-readonly',
                    [FILE_ATTRS.url]: url,
                    [FILE_ATTRS.name]: name,
                    [FILE_ATTRS.type]: type,
                    [FILE_ATTRS.size]: size.toString(),
                    style: 'padding: 8px 12px; border: 1px solid #d9d9d9; border-radius: 6px; margin: 8px 0; background: #fafafa; cursor: pointer; display: inline-block;',
                    title: `${name} (${type}) - Click to download`,
                    onclick: url ? `window.open('${url}', '_blank')` : undefined,
                };

                // Create a more styled content for read-only mode
                const icon = type.startsWith('image/') ? '🖼️' : type === 'application/pdf' ? '📄' : '📎';

                console.log('🔥 toDOM returning:', ['div', attrs, `${icon} ${name}`]);
                return ['div', attrs, `${icon} ${name}`];
            },
        };
    }

    /**
     * Renders a React Component in place of the dom node spec
     */
    ReactComponent: ComponentType<NodeViewComponentProps> = (props) => <FileNodeView {...props} />;

    createCommands() {
        return {
            /**
             * Inserts a file node at the current selection
             */
            insertFileNode: (attrs: FileNodeAttributes): CommandFunction => {
                return ({ tr, dispatch }) => {
                    const node = this.type.create(attrs);
                    const transaction = tr.replaceSelectionWith(node);
                    dispatch?.(transaction);
                    return true;
                };
            },
        };
    }
}

const decoratedExt = extension<FileDragDropOptions>({
    handlerKeys: ['onFileUpload'],
    defaultOptions: {
        supportedTypes: [
            'image/jpeg',
            'image/png',
            'image/gif',
            'image/webp',
            'application/pdf',
            'text/plain',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        ],
    },
})(FileDragDropExtension);

export { decoratedExt as FileDragDropExtension };
