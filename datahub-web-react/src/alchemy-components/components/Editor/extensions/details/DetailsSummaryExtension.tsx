/* eslint-disable class-methods-use-this */
import {
    ApplySchemaAttributes,
    ExtensionTag,
    NodeExtension,
    NodeExtensionSpec,
    NodeSpecOverride,
    extension,
} from '@remirror/core';

/**
 * Provides ProseMirror schema support for the `<summary>` child of a `<details>` element.
 *
 * Intentionally **not** added to the `block` group so it is only valid as the first
 * child of a `details` node, preventing it from appearing elsewhere in the document.
 */
class DetailsSummaryExtensionClass extends NodeExtension {
    get name() {
        return 'detailsSummary' as const;
    }

    createTags() {
        return [ExtensionTag.Block];
    }

    createNodeSpec(extra: ApplySchemaAttributes, override: NodeSpecOverride): NodeExtensionSpec {
        return {
            ...override,
            // no `group: 'block'` — only valid inside a details node
            content: 'inline*',
            defining: true,
            attrs: { ...extra.defaults() },
            parseDOM: [
                {
                    tag: 'summary',
                    getAttrs: (node) => extra.parse(node as HTMLElement),
                },
                ...(override.parseDOM ?? []),
            ],
            toDOM: (node) => ['summary', extra.dom(node), 0],
        };
    }
}

const decoratedDetailsSummary = extension({
    staticKeys: [],
    handlerKeys: [],
    customHandlerKeys: [],
})(DetailsSummaryExtensionClass);

export { decoratedDetailsSummary as DetailsSummaryExtension };
