declare module 'virtual:i18n-locale-loaders' {
    const loaders: Record<string, Record<string, () => Promise<{ default: Record<string, Record<string, unknown>> }>>>;
    export const namespaceGroups: Record<string, string>;
    export const coreNamespaces: string[];
    export const namespacesByGroup: Record<string, string[]>;
    export default loaders;
}
