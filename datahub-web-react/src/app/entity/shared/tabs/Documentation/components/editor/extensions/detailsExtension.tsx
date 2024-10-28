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

class DetailsExtension extends NodeExtension {
    get name() {
        return 'details' as const;
    }

    createTags() {
        return [ExtensionTag.Block];
    }

    createNodeSpec(extra: ApplySchemaAttributes, override: MarkSpecOverride): NodeExtensionSpec {
        return {
            content: 'block*',
            draggable: false,
            ...override,
            attrs: extra.defaults(),
            parseDOM: [
                {
                    tag: 'details',
                    getAttrs: extra.parse,
                },
                ...(override.parseDOM ?? []),
            ],
            toDOM: (node) => {
                return ['details', extra.dom(node), 0];
            },
        };
    }
}

const decoratedExt = extension({ defaultPriority: ExtensionPriority.Highest })(DetailsExtension);
export { decoratedExt as DetailsExtension };
