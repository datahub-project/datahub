declare module 'refractor/core' {
    interface RefractorCore {
        alias: (name: string, aliasNames: string | readonly string[]) => void;
        listLanguages: () => string[];
        registered: (name: string) => boolean;
    }

    const refractor: RefractorCore;
    export default refractor;
}

declare module 'react-syntax-highlighter/dist/esm/prism-light' {
    import type { PrismLightComponent } from '@components/components/CodeBlock/prismLightTypes';

    /**
     * allowJs binds the installed prism-light.js, so this declaration does not
     * replace that file's inferred type. Callers assert the import to PrismLightComponent.
     * `registerLanguage` ignores `name` and registers `language.displayName`.
     */
    const SyntaxHighlighter: PrismLightComponent;
    export default SyntaxHighlighter;
}

declare module 'react-syntax-highlighter/dist/esm/languages/prism/*' {
    import type { RefractorSyntax } from 'refractor';

    const language: RefractorSyntax;
    export default language;
}
