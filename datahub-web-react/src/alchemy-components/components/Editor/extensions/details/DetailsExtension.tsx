/* eslint-disable class-methods-use-this */
import {
    ApplySchemaAttributes,
    ExtensionTag,
    NodeExtension,
    NodeExtensionSpec,
    NodeSpecOverride,
    extension,
} from '@remirror/core';
import { Plugin, PluginKey } from 'prosemirror-state';
import { EditorView } from 'prosemirror-view';

import { DETAILS_TOGGLE_META } from '@components/components/Editor/types';

/**
 * Provides ProseMirror schema support for HTML `<details>` disclosure widgets.
 *
 * The open/closed state is stored as a node attribute so ProseMirror's DOM
 * reconciliation always renders the correct `<details open>` / `<details>`
 * element.  Clicking `<summary>` dispatches a transaction that flips the
 * attribute; `toDOM` re-renders accordingly.
 *
 * Markdown storage format: raw HTML `<details>` blocks (GitHub-compatible).
 * The `htmlToMarkdown` turndown rule preserves them as `outerHTML`; `markdownToHtml`
 * (via `marked`) passes them through as HTML blocks unchanged.
 *
 * Must be registered alongside `DetailsSummaryExtension` in the editor's extension list.
 */
class DetailsExtensionClass extends NodeExtension {
    get name() {
        return 'details' as const;
    }

    createTags() {
        return [ExtensionTag.Block];
    }

    createNodeSpec(extra: ApplySchemaAttributes, override: NodeSpecOverride): NodeExtensionSpec {
        return {
            ...override,
            group: 'block',
            // exactly one summary chip followed by at least one block of body content
            content: 'detailsSummary block+',
            defining: true,
            attrs: {
                ...extra.defaults(),
                open: { default: false },
            },
            parseDOM: [
                {
                    tag: 'details',
                    getAttrs: (node) => ({
                        ...extra.parse(node as HTMLElement),
                        open: (node as HTMLElement).hasAttribute('open'),
                    }),
                },
                ...(override.parseDOM ?? []),
            ],
            toDOM: (node) => {
                const domAttrs = extra.dom(node);
                if (node.attrs.open) {
                    return ['details', { ...domAttrs, open: '' }, 0];
                }
                return ['details', domAttrs, 0];
            },
        };
    }

    createExternalPlugins(): Plugin[] {
        return [
            new Plugin({
                key: new PluginKey('detailsToggle'),
                props: {
                    handleDOMEvents: {
                        // When the editor is NOT yet focused, prevent mousedown on <summary>
                        // from transferring focus to the editor.  Without this, the mousedown
                        // fires onFocus on the EditorSection wrapper which triggers the toolbar
                        // appear / layout-shift / re-render lag ("edit mode" activation).
                        // When the editor IS already focused the user is actively editing, so
                        // we let mousedown through normally so they can place the cursor inside
                        // the summary text.
                        mousedown: (view: EditorView, event: MouseEvent) => {
                            if (!view.hasFocus() && (event.target as HTMLElement).closest('summary')) {
                                event.preventDefault();
                                return true;
                            }
                            return false;
                        },
                        click: (view: EditorView, event: MouseEvent) => {
                            const target = event.target as HTMLElement;
                            if (!target.closest('summary')) return false;

                            const detailsElem = target.closest('details');
                            if (!detailsElem) return false;

                            // Map the DOM position to a ProseMirror document position,
                            // then walk up to find the enclosing `details` node.
                            const insidePos = view.posAtDOM(detailsElem, 0);
                            const $pos = view.state.doc.resolve(insidePos);

                            for (let { depth } = $pos; depth > 0; depth--) {
                                const nodeAtDepth = $pos.node(depth);
                                if (nodeAtDepth.type.name === 'details') {
                                    const nodePos = $pos.before(depth);
                                    view.dispatch(
                                        view.state.tr
                                            .setNodeMarkup(nodePos, null, {
                                                ...nodeAtDepth.attrs,
                                                open: !nodeAtDepth.attrs.open,
                                            })
                                            .setMeta(DETAILS_TOGGLE_META, true),
                                    );
                                    // Prevent the native toggle — we handle state ourselves.
                                    event.preventDefault();
                                    return true;
                                }
                            }

                            return false;
                        },
                    },
                },
            }),
        ];
    }
}

const decoratedDetails = extension({
    staticKeys: [],
    handlerKeys: [],
    customHandlerKeys: [],
})(DetailsExtensionClass);

export { decoratedDetails as DetailsExtension };
