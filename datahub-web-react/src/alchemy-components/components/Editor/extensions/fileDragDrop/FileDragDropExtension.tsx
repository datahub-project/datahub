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
    generateFileId,
    getFileTypeFromFilename,
    getFileTypeFromUrl,
    isFileUrl,
    validateFile,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { notification } from '@components/components/Notification/notification';

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
                console.error(validation.error);
                notification.error({
                    message: 'Upload Failed',
                    description: validation.displayError || validation.error,
                });
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
                    console.error(uploadError);
                    this.removeNode(view, placeholderAttrs.id);
                    notification.error({
                        message: 'Upload Failed',
                        description: 'Something went wrong',
                    });
                }
            }
        } catch (error) {
            console.error(error);
            notification.error({
                message: 'Upload Failed',
                description: 'Something went wrong',
            });
        }
    }

    public updateNodeWithUrl(view: EditorView, nodeId: string, url: string): void {
        const { nodePos, nodeToUpdate } = this.findNodeById(view.state, nodeId);

        if (!nodePos || !nodeToUpdate) return;

        const { name, type } = nodeToUpdate.attrs;

        if (type.startsWith('image/')) {
            this.replaceWithImageNode(view, nodePos, nodeToUpdate, url, name);
        } else {
            this.updateFileNodeUrl(view, nodePos, nodeToUpdate, url);
        }
    }

    public removeNode(view: EditorView, nodeId: string) {
        const { nodePos, nodeToUpdate } = this.findNodeById(view.state, nodeId);
        if (!nodePos || !nodeToUpdate) return;

        const updatedTransaction = view.state.tr.delete(nodePos, nodePos + nodeToUpdate.nodeSize);
        view.dispatch(updatedTransaction);
    }

    private findNodeById(state: any, nodeId: string): { nodePos: number | null; nodeToUpdate: any } {
        let nodePos: number | null = null;
        let nodeToUpdate: any = null;

        state.doc.descendants((descendantNode: any, descendantPos: number) => {
            if (
                descendantNode.type === this.type &&
                descendantNode.attrs.id === nodeId &&
                descendantNode.attrs.url === ''
            ) {
                nodePos = descendantPos;
                nodeToUpdate = descendantNode;
                return false; // Stop searching
            }
            return true; // Continue searching
        });

        return { nodePos, nodeToUpdate };
    }

    private replaceWithImageNode(
        view: EditorView,
        nodePos: number,
        nodeToUpdate: any,
        url: string,
        name: string,
    ): void {
        const imageNode = view.state.schema.nodes.image?.create({
            src: url,
            alt: name,
            title: name,
        });

        if (imageNode) {
            const replaceTransaction = view.state.tr.replaceWith(nodePos, nodePos + nodeToUpdate.nodeSize, imageNode);
            view.dispatch(replaceTransaction);
        }
    }

    private updateFileNodeUrl(view: EditorView, nodePos: number, nodeToUpdate: any, url: string): void {
        const updatedAttrs = { ...nodeToUpdate.attrs, url };
        const updateTransaction = view.state.tr.setNodeMarkup(nodePos, null, updatedAttrs);
        view.dispatch(updateTransaction);
    }

    private parseFileNode(node: string | Node, extra: ApplySchemaAttributes): any {
        if (!isElementDomNode(node)) {
            return false;
        }

        const url = node.getAttribute(FILE_ATTRS.url) || '';
        const name = node.getAttribute(FILE_ATTRS.name) || '';
        const type = node.getAttribute(FILE_ATTRS.type) || '';
        const size = parseInt(node.getAttribute(FILE_ATTRS.size) || '0', 10);
        const id = node.getAttribute(FILE_ATTRS.id) || '';

        return { ...extra.parse(node), url, name, type, size, id };
    }

    private parseFileLink(node: string | Node, extra: ApplySchemaAttributes): any {
        if (!isElementDomNode(node)) {
            return false;
        }

        const href = node.getAttribute('href');
        const text = node.textContent || '';

        // Check if this is a file link
        if (href && isFileUrl(href)) {
            const type = getFileTypeFromUrl(href) || getFileTypeFromFilename(text) || '';
            const size = 0; // We don't store size in standard markdown
            const id = generateFileId();

            return { ...extra.parse(node), url: href, name: text, type, size, id };
        }

        return false;
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
                    getAttrs: (node: string | Node) => this.parseFileNode(node, extra),
                },
                {
                    tag: 'a',
                    getAttrs: (node: string | Node) => this.parseFileLink(node, extra),
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
                };

                return ['div', attrs, name];
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
        onFileUpload: async (_file: File) => '',
        supportedTypes: SUPPORTED_FILE_TYPES,
    },
})(FileDragDropExtension);

export { decoratedExt as FileDragDropExtension };
