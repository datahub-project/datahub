/* eslint-disable class-methods-use-this */
import React, { ComponentType } from 'react';
import { Plugin } from '@remirror/pm/state';
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

class DataHubMentionsExtension extends NodeExtension<DataHubMentionsOptions> {
    get name() {
        return 'datahubMention' as const;
    }

    createTags() {
        return [ExtensionTag.InlineNode, ExtensionTag.Behavior];
    }

    createExternalPlugins(): Plugin[] {
        return autocomplete({
            triggers: [{ name: 'mention', trigger: '@', cancelOnFirstSpace: false }],
            reducer: (action) => this.options.handleEvents?.(action) || false,
        });
    }

    createNodeSpec(extra: ApplySchemaAttributes, override: NodeSpecOverride): NodeExtensionSpec {
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

    ReactComponent: ComponentType<NodeViewComponentProps> = (props) => <MentionsNodeView {...props} />;

    createCommands() {
        return {
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
