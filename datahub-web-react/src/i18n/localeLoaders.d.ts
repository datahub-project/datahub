declare module 'virtual:i18n-locale-loaders' {
    const loaders: Record<string, () => Promise<{ default: Record<string, Record<string, unknown>> }>>;
    export default loaders;
}
