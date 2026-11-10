/**
 * ESLint rule: use-app-wrappers
 *
 * Some alchemy components get app behavior (DataHub mentions, error reporting…) from an app
 * wrapper. App code must render the wrapper, so it may not import the alchemy component itself:
 * - ❌ import { Editor } from '@components'
 * - ❌ import { Editor } from '@components/components/Editor/EditorImpl' (any file of the component)
 * - ✅ import { Editor } from '@app/sharedV2/editor/Editor'
 * - ❌ import * as EditorModule from '@components/components/Editor/Editor', export * from it, or import() it
 * - ✅ import { EditorProps } from '@components/components/Editor/types' (other names)
 *
 * A namespace import, `export *` or `import()` of the alchemy root is not flagged: it does not name
 * the component, and the root exports every other alchemy component too.
 *
 * Alchemy files and the component's own wrappers are exempt.
 *
 * Options: { components: [{ name: 'Editor', folder: 'Editor', wrappers: ['src/app/sharedV2/editor/Editor.tsx'] }] }
 * - name: the exported alchemy component
 * - folder: its folder under src/alchemy-components/components
 * - wrappers: the app files that may import it; the first is the one app code should use, the others
 *   are wrappers that need the raw alchemy component (e.g. a different set of slots)
 */

const ALCHEMY_ROOTS = ['@components', '@src/alchemy-components'];

function importsFromFolder(source, folder) {
    return ALCHEMY_ROOTS.some((root) => {
        const folderPath = `${root}/components/${folder}`;
        return source === folderPath || source.startsWith(`${folderPath}/`);
    });
}

function importsFromComponent(source, folder) {
    return ALCHEMY_ROOTS.includes(source) || importsFromFolder(source, folder);
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
                            properties: {
                                name: { type: 'string' },
                                folder: { type: 'string' },
                                wrappers: { type: 'array', items: { type: 'string' }, minItems: 1 },
                            },
                            required: ['name', 'folder', 'wrappers'],
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

        const isOwnWrapper = ({ wrappers }) =>
            wrappers.some((wrapper) => filename === wrapper || filename.endsWith(`/${wrapper}`));
        const components = (context.options[0]?.components ?? []).filter((component) => !isOwnWrapper(component));
        if (components.length === 0) return {};

        function report(node, component) {
            context.report({
                node,
                message: `Import ${component.name} from '${wrapperImportPath(component.wrappers[0])}': the app wrapper adds app behavior to the alchemy component.`,
            });
        }

        function check(node, importedName) {
            const source = node.parent.source?.value;
            if (typeof source !== 'string') return;
            const component = components.find(
                ({ name, folder }) => name === importedName && importsFromComponent(source, folder),
            );
            if (component) report(node, component);
        }

        // Namespace imports, `export *` and `import()` reach every export of the module.
        function checkWholeModule(node, sourceNode) {
            const source = sourceNode?.value;
            if (typeof source !== 'string') return;
            const component = components.find(({ folder }) => importsFromFolder(source, folder));
            if (component) report(node, component);
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
            ImportNamespaceSpecifier(node) {
                if (node.parent.importKind === 'type') return;
                checkWholeModule(node, node.parent.source);
            },
            ExportAllDeclaration(node) {
                if (node.exportKind === 'type') return;
                checkWholeModule(node, node.source);
            },
            ImportExpression(node) {
                checkWholeModule(node, node.source);
            },
        };
    },
};
