declare module 'refractor/core' {
    interface RefractorCore {
        alias: (name: string, aliasNames: string | readonly string[]) => void;
    }

    const refractor: RefractorCore;
    export default refractor;
}

declare module 'react-syntax-highlighter/dist/esm/languages/prism/*' {
    const language: unknown;
    export default language;
}
