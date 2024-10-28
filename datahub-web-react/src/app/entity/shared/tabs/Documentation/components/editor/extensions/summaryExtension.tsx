/* eslint-disable class-methods-use-this */
import {
    ApplySchemaAttributes,
    extension,
    ExtensionTag,
    ExtensionPriority,
    MarkSpecOverride,
    NodeExtension,
    NodeExtensionSpec,
} from '@remirror/core';

class SummaryExtension extends NodeExtension {
    get name() {
        return 'summary' as const;
    }

    createTags() {
        return [ExtensionTag.Block, ExtensionTag.LastNodeCompatible];
    }

    createNodeSpec(extra: ApplySchemaAttributes, override: MarkSpecOverride): NodeExtensionSpec {
        return {
            content: 'inline*',
            draggable: false,
            ...override,
            attrs: extra.defaults(),
            parseDOM: [
                {
                    tag: 'summary',
                    getAttrs: extra.parse,
                },
                ...(override.parseDOM ?? []),
            ],
            toDOM: (node) => {
                return ['summary', extra.dom(node), 0];
            },
        };
    }
}

const decoratedExt = extension({ defaultPriority: ExtensionPriority.Highest })(SummaryExtension);
export { decoratedExt as SummaryExtension };
