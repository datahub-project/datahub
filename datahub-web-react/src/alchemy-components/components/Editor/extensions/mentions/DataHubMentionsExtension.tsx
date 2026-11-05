/* eslint-disable class-methods-use-this */
import {
    ApplySchemaAttributes,
    CommandFunction,
    ExtensionTag,
    FromToProps,
    Handler,
    NodeExtension,
    NodeExtensionSpec,
    NodeSpecOverride,
    ProsemirrorAttributes,
    Static,
    extension,
    isElementDomNode,
    omitExtraAttributes,
} from '@remirror/core';
import { NodeViewComponentProps } from '@remirror/react';
import autocomplete, {
    ActiveAutocompleteState,
    AutocompleteAction,
    AutocompleteState,
    KEEP_OPEN,
    pluginKey as acPluginKey,
} from 'prosemirror-autocomplete';
import { Plugin, PluginKey } from 'prosemirror-state';
import type { EditorView } from 'prosemirror-view';
import React, { ComponentType } from 'react';

import { MentionNode } from '@components/components/Editor/extensions/mentions/MentionNode';
import { DATAHUB_MENTION_ATTRS } from '@components/components/Editor/extensions/mentions/constants';

export { DATAHUB_MENTION_ATTRS };

type DataHubAtomNodeAttributes = ProsemirrorAttributes & {
    name: string;
    urn: string;
};

interface DataHubMentionsOptions {
    /** Renders a mention node; without it a mention renders as `@name`. */
    mentionNodeView?: Static<ComponentType<NodeViewComponentProps>>;
    /** Return `KEEP_OPEN` from an `enter` action to consume the key without closing the menu. */
    handleEvents?: Handler<(action: AutocompleteAction) => boolean | typeof KEEP_OPEN>;
    /**
     * Sees key presses while an @-mention is being typed, before the autocomplete plugin does, e.g.
     * to repurpose Backspace on an empty query. Return true to consume the key.
     */
    handleKeyDown?: Handler<(event: KeyboardEvent, state: ActiveAutocompleteState) => boolean>;
}

/**
 * The DataHub mentions extensions wraps @-mentions as a prosemirror node. It adds capability to
 * use and render @-mentions within the editor. The implementation was inspired by Notion where a
 * dedicated search bar is displayed for users to search. Mentions cannot be edited once being inserted
 * into the document.
 *
 * This Remirror plugin is simply a wrapper on top of `prosemirror-autocomplete` which provides the
 * suggestion engine, allowing spaces to be used within the search bar.
 */
class DataHubMentionsExtension extends NodeExtension<DataHubMentionsOptions> {
    get name() {
        return 'datahubMention' as const;
    }

    createTags() {
        return [ExtensionTag.InlineNode, ExtensionTag.Behavior];
    }

    /**
     * Add the 'prosemirror-autocomplete' plugin to the editor, preceded by a key handler: ProseMirror
     * asks plugins in order, so `handleKeyDown` sees keys before the autocomplete plugin does.
     */
    createExternalPlugins(): Plugin[] {
        const keyHandler = new Plugin({
            key: new PluginKey('datahubMentionKeys'),
            props: {
                handleKeyDown: (view: EditorView, event: KeyboardEvent) => {
                    const autocompleteState: AutocompleteState | undefined = acPluginKey.getState(view.state);
                    if (!autocompleteState?.active) return false;
                    return this.options.handleKeyDown?.(event, autocompleteState) || false;
                },
            },
        });
        return [
            keyHandler,
            ...autocomplete({
                triggers: [{ name: 'mention', trigger: '@', cancelOnFirstSpace: false }],
                reducer: (action) => this.options.handleEvents?.(action) || false,
            }),
        ];
    }

    createNodeSpec(extra: ApplySchemaAttributes, override: Partial<NodeSpecOverride>): NodeExtensionSpec {
        return {
            inline: true,
            marks: '',
            selectable: true,
            draggable: true,
            atom: true,
            ...override,
            attrs: {
                ...extra.defaults(),
                urn: {},
                name: {},
            },
            parseDOM: [
                {
                    tag: `span[${DATAHUB_MENTION_ATTRS.urn}]`,
                    getAttrs: (node: string | Node) => {
                        if (!isElementDomNode(node)) {
                            return false;
                        }

                        const urn = node.getAttribute(DATAHUB_MENTION_ATTRS.urn);
                        const nameAttr = node.getAttribute(DATAHUB_MENTION_ATTRS.name);
                        const name = nameAttr || node.textContent?.replace(/^@/, '') || urn;

                        return { ...extra.parse(node), urn, name };
                    },
                },
                ...(override.parseDOM ?? []),
            ],
            toDOM: (node) => {
                const { name, urn } = omitExtraAttributes(node.attrs, extra) as DataHubAtomNodeAttributes;

                const attrs = {
                    ...extra.dom(node),
                    class: 'mentions',
                    [DATAHUB_MENTION_ATTRS.urn]: urn,
                    [DATAHUB_MENTION_ATTRS.name]: name,
                };

                return ['span', attrs, `@${name}`];
            },
        };
    }

    /**
     * Renders a React Component in place of the dom node spec
     */
    ReactComponent: ComponentType<NodeViewComponentProps> = (props) => (
        <MentionNode {...props} view={this.options.mentionNodeView} />
    );

    createCommands() {
        return {
            /**
             * Creates a mention at the provided range.
             *
             * @param attrs - the attributes that should be passed through. Required values are 'name' and 'urn'.
             * @param range - the range of the selection that would be replaced.
             */
            createDataHubMention: (attrs: DataHubAtomNodeAttributes, range?: FromToProps): CommandFunction => {
                return ({ state, tr, dispatch }) => {
                    const acState: ActiveAutocompleteState = acPluginKey.getState(state);
                    const { from = 0, to = 0 } = range ?? acState.range ?? {};
                    tr.replaceRangeWith(from, to, this.type.create(attrs));
                    // Append a space after the mention to fix Safari cursor positioning bug.
                    // Safari has a known issue where the cursor jumps to the start of the line
                    // when an atom node is the last element. Appending text gives the cursor
                    // a text node to position after. See: https://github.com/ProseMirror/prosemirror/issues/1165
                    tr.insertText(' ');
                    dispatch?.(tr);

                    return true;
                };
            },
        };
    }
}

const decoratedExt = extension<DataHubMentionsOptions>({
    defaultOptions: { mentionNodeView: undefined },
    staticKeys: ['mentionNodeView'],
    handlerKeys: ['handleEvents', 'handleKeyDown'],
    customHandlerKeys: [],
})(DataHubMentionsExtension);
export { decoratedExt as DataHubMentionsExtension };
