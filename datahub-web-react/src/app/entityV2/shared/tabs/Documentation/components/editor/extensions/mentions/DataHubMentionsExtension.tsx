/* eslint-disable class-methods-use-this */
import React, { ComponentType } from 'react';
import type { Plugin } from 'prosemirror-state';
import {
    ApplySchemaAttributes,
    CommandFunction,
    extension,
    ExtensionTag,
    FromToProps,
    Handler,
    isElementDomNode,
    NodeExtension,
    NodeExtensionSpec,
    NodeSpecOverride,
    omitExtraAttributes,
    ProsemirrorAttributes,
} from '@remirror/core';
import { NodeViewComponentProps } from '@remirror/react';
import autocomplete, {
    ActiveAutocompleteState,
    AutocompleteAction,
    pluginKey as acPluginKey,
} from 'prosemirror-autocomplete';
import { MentionsNodeView } from './MentionsNodeView';

export const DATAHUB_MENTION_ATTRS = {
    urn: 'data-datahub-mention-urn',
};

type DataHubAtomNodeAttributes = ProsemirrorAttributes & {
    name: string;
    urn: string;
};

interface DataHubMentionsOptions {
    handleEvents?: Handler<(action: AutocompleteAction) => boolean>;
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
     * Add the 'prosemirror-autocomplete' plugin to the editor.
     */
    createExternalPlugins(): Plugin[] {
        return autocomplete({
            triggers: [{ name: 'mention', trigger: '@', cancelOnFirstSpace: false }],
            reducer: (action) => this.options.handleEvents?.(action) || false,
        });
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
                        const name = node.textContent?.replace(/^@/, '') || urn;

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
                };

                return ['span', attrs, `@${name}`];
            },
        };
    }

    /**
     * Renders a React Component in place of the dom node spec
     */
    ReactComponent: ComponentType<NodeViewComponentProps> = (props) => <MentionsNodeView {...props} />;

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
                    dispatch?.(tr);

                    return true;
                };
            },
        };
    }
}

const decoratedExt = extension<DataHubMentionsOptions>({ handlerKeys: ['handleEvents'] })(DataHubMentionsExtension);
export { decoratedExt as DataHubMentionsExtension };
