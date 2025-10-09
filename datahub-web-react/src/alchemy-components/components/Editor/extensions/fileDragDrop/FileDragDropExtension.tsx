/* eslint-disable class-methods-use-this */
import {
    ApplySchemaAttributes,
    CommandFunction,
    ExtensionPriority,
    ExtensionTag,
    NodeExtension,
    NodeExtensionSpec,
    NodeSpecOverride,
    extension,
    isElementDomNode,
    omitExtraAttributes,
} from '@remirror/core';
import { NodeViewComponentProps } from '@remirror/react';
import { Plugin, PluginKey } from 'prosemirror-state';
import { EditorView } from 'prosemirror-view';
import React, { ComponentType } from 'react';

import { FileNodeView } from '@components/components/Editor/extensions/fileDragDrop/FileNodeView';
import { 
    FILE_ATTRS, 
    FileNodeAttributes, 
    SUPPORTED_FILE_TYPES, 
    createFileNodeAttributes, 
    validateFile,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';

interface FileDragDropOptions {
    onFileUpload?: (file: File) => Promise<string>;
    supportedTypes?: string[];
}

/**
 * The FileDragDrop extension allows users to drag and drop files into the editor.
 * It creates file nodes that render differently based on file type (images, PDFs, etc.)
 * and handles file uploads to S3 via pre-signed URLs.
 */
class FileDragDropExtension extends NodeExtension<FileDragDropOptions> {
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
                            return this.handleDrop(view, event);
                        },
                        dragover: (view: EditorView, event: DragEvent) => {
                            if (event.dataTransfer?.types.includes('Files')) {
                                event.preventDefault();
                                if (event.dataTransfer) {
                                    // eslint-disable-next-line no-param-reassign
                                    event.dataTransfer.dropEffect = 'copy';
                                }
                                return true;
                            }
                            return false;
                        },
                        dragenter: (view: EditorView, event: DragEvent) => {
                            if (event.dataTransfer?.types.includes('Files')) {
                                event.preventDefault();
                                return true;
                            }
                            return false;
                        },
                        dragleave: (_view: EditorView, _event: DragEvent) => {
                            return false;
                        },
                    },
                },
            }),
        ];
    }

    private async handleDrop(view: EditorView, event: DragEvent): Promise<boolean> {
        event.preventDefault();
        event.stopPropagation();

        const { files } = event.dataTransfer || {};
        if (!files || files.length === 0) {
            return false;
        }

        const supportedTypes = this.options.supportedTypes || SUPPORTED_FILE_TYPES;
        const dropPosition = this.getDropPosition(view, event);

        // Process each file
        const fileArray = Array.from(files);
        const processPromises = fileArray.map(async (file) => {
            const validation = validateFile(file, { allowedTypes: supportedTypes });
            if (!validation.isValid) {
                return; // Skip invalid files
            }

            await this.processFile(file, view, dropPosition);
        });

        await Promise.all(processPromises);

        return true;
    }

    private getDropPosition(view: EditorView, event: DragEvent): number {
        const coordinates = view.posAtCoords({ left: event.clientX, top: event.clientY });
        return coordinates?.pos ?? view.state.selection.from;
    }

    private async processFile(file: File, view: EditorView, position: number): Promise<void> {
        try {
            // Create placeholder node
            const placeholderAttrs = createFileNodeAttributes(file);
            const node = this.type.create(placeholderAttrs);
            const transaction = view.state.tr.insert(position, node);
            view.dispatch(transaction);

            // Upload file if handler is provided
            if (this.options.onFileUpload) {
                try {
                    const finalUrl = await this.options.onFileUpload(file);
                    this.updateNodeWithUrl(view, placeholderAttrs.id, finalUrl);
                } catch (uploadError) {
                    console.error('Upload failed:', uploadError);
                }
            }
        } catch (error) {
            console.error('Error processing file:', error);
        }
    }

    private updateNodeWithUrl(view: EditorView, nodeId: string, url: string): void {
        const currentState = view.state;
        let nodePos: number | null = null;

        // Find the node by ID
        currentState.doc.descendants((descendantNode, descendantPos) => {
            if (descendantNode.type === this.type && 
                descendantNode.attrs.id === nodeId && 
                descendantNode.attrs.url === '') {
                nodePos = descendantPos;
                return false; // Stop searching
            }
            return true; // Continue searching
        });

        if (nodePos !== null) {
            const node = currentState.doc.nodeAt(nodePos);
            if (node) {
                const updatedAttrs = { ...node.attrs, url };
                const updateTransaction = currentState.tr.setNodeMarkup(nodePos, null, updatedAttrs);
                view.dispatch(updateTransaction);
            }
        }
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
                id: { default: '' },
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
                        const id = node.getAttribute(FILE_ATTRS.id) || '';

                        return { ...extra.parse(node), url, name, type, size, id };
                    },
                },
                ...(override.parseDOM ?? []),
            ],
            toDOM: (node) => {
                const { url, name, type, size, id } = omitExtraAttributes(node.attrs, extra) as FileNodeAttributes;

                const attrs = {
                    ...extra.dom(node),
                    class: 'file-node file-node-readonly',
                    [FILE_ATTRS.url]: url,
                    [FILE_ATTRS.name]: name,
                    [FILE_ATTRS.type]: type,
                    [FILE_ATTRS.size]: size.toString(),
                    [FILE_ATTRS.id]: id,
                    style: 'padding: 8px 12px; border: 1px solid #d9d9d9; border-radius: 6px; margin: 8px 0; background: #fafafa; cursor: pointer; display: inline-block;',
                    title: `${name} (${type}) - Click to download`,
                };

                // Create a more styled content for read-only mode
                let icon = '📎';
                if (type.startsWith('image/')) {
                    icon = '🖼️';
                } else if (type === 'application/pdf') {
                    icon = '📄';
                }

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
    staticKeys: [],
    handlerKeys: [],
    customHandlerKeys: [],
    defaultOptions: {
        onFileUpload: undefined,
        supportedTypes: SUPPORTED_FILE_TYPES,
    },
})(FileDragDropExtension);

export { decoratedExt as FileDragDropExtension };
