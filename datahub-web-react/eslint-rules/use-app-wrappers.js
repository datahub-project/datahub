/**
 * ESLint rule: use-app-wrappers
 *
 * Some alchemy components get app behavior (DataHub mentions, error reporting…) from an app
 * wrapper. App code must render the wrapper, so it may not import the alchemy component itself:
 * - ❌ import { Editor } from '@components'
 * - ❌ import { Editor } from '@components/components/Editor/EditorImpl' (any file of the component)
 * - ✅ import { Editor } from '@app/sharedV2/editor/Editor'
 * - ✅ import { EditorProps } from '@components/components/Editor/types' (other names)
 *
 * Alchemy files and the component's own wrapper are exempt.
 *
 * Options: { components: [{ name: 'Editor', wrapper: 'src/app/sharedV2/editor/Editor.tsx' }] }
 * `name` is both the exported component and its folder under src/alchemy-components/components.
 */

const ALCHEMY_ROOTS = ['@components', '@src/alchemy-components'];

function importsFromComponent(source, name) {
    return ALCHEMY_ROOTS.some((root) => {
        const folder = `${root}/components/${name}`;
        return source === root || source === folder || source.startsWith(`${folder}/`);
    });
}

function wrapperImportPath(wrapper) {
    return wrapper.replace(/^src\/app\//, '@app/').replace(/\.tsx?$/, '');
}

module.exports = {
    meta: {
        type: 'problem',
        docs: {
            description: 'Require the app wrapper instead of importing alchemy components that have one.',
        },
        schema: [
            {
                type: 'object',
                properties: {
                    components: {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: { name: { type: 'string' }, wrapper: { type: 'string' } },
                            required: ['name', 'wrapper'],
                            additionalProperties: false,
                        },
                    },
                },
                additionalProperties: false,
            },
        ],
    },
    create(context) {
        const filename = context.getFilename().replace(/\\/g, '/');
        if (filename.includes('/src/alchemy-components/') || filename.startsWith('src/alchemy-components/')) {
            return {};
        }

        const isOwnWrapper = ({ wrapper }) => filename === wrapper || filename.endsWith(`/${wrapper}`);
        const components = (context.options[0]?.components ?? []).filter((component) => !isOwnWrapper(component));
        if (components.length === 0) return {};

        function check(node, importedName) {
            const source = node.parent.source?.value;
            if (typeof source !== 'string') return;
            const component = components.find(
                ({ name }) => name === importedName && importsFromComponent(source, name),
            );
            if (!component) return;
            context.report({
                node,
                message: `Import ${component.name} from '${wrapperImportPath(component.wrapper)}': the app wrapper adds app behavior to the alchemy component.`,
            });
        }

        return {
            ImportSpecifier(node) {
                if (node.parent.importKind === 'type' || node.importKind === 'type') return;
                check(node, node.imported.name);
            },
            ExportSpecifier(node) {
                if (node.parent.exportKind === 'type' || node.exportKind === 'type') return;
                check(node, node.local.name);
            },
        };
    },
};
