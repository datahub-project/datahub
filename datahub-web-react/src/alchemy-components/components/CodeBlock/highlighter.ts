import asyncLanguages from 'react-syntax-highlighter/dist/esm/async-languages/prism';
import bash from 'react-syntax-highlighter/dist/esm/languages/prism/bash';
import graphql from 'react-syntax-highlighter/dist/esm/languages/prism/graphql';
import json from 'react-syntax-highlighter/dist/esm/languages/prism/json';
import markdown from 'react-syntax-highlighter/dist/esm/languages/prism/markdown';
import protobuf from 'react-syntax-highlighter/dist/esm/languages/prism/protobuf';
import python from 'react-syntax-highlighter/dist/esm/languages/prism/python';
import sql from 'react-syntax-highlighter/dist/esm/languages/prism/sql';
import typescript from 'react-syntax-highlighter/dist/esm/languages/prism/typescript';
import yaml from 'react-syntax-highlighter/dist/esm/languages/prism/yaml';
import prismLight from 'react-syntax-highlighter/dist/esm/prism-light';
import type { RefractorSyntax } from 'refractor/core';
import refractor from 'refractor/core';

import type { PrismLightComponent } from '@components/components/CodeBlock/prismLightTypes';

// allowJs binds prism-light.js, so the import is untyped. registerLanguage
// ignores `name` and uses grammar.displayName.
const SyntaxHighlighter = prismLight as unknown as PrismLightComponent;

// prism-light only loads refractor's core. These cover what entity pages show
// most often; anything else is fetched by loadHighlightLanguage.
const GRAMMARS = [bash, graphql, json, markdown, protobuf, python, sql, typescript, yaml];

GRAMMARS.forEach((grammar) => {
    SyntaxHighlighter.registerLanguage(grammar.displayName, grammar);
});

type LanguageLoader = (register: (name: string, grammar: RefractorSyntax) => void) => Promise<void>;

// One import() per grammar. This is the map PrismAsyncLight uses.
const LANGUAGE_LOADERS = asyncLanguages as Record<string, LanguageLoader>;

const pendingLoads = new Map<string, Promise<boolean>>();

/**
 * Registers a grammar that is not in the eager set.
 *
 * @param language - Canonical Prism language id
 * @returns True once the grammar is registered; false when no grammar exists or the chunk failed
 */
export function loadHighlightLanguage(language: string): Promise<boolean> {
    if (refractor.registered(language)) {
        return Promise.resolve(true);
    }
    if (!Object.hasOwn(LANGUAGE_LOADERS, language)) {
        return Promise.resolve(false);
    }
    let pending = pendingLoads.get(language);
    if (!pending) {
        pending = LANGUAGE_LOADERS[language]((name, grammar) => SyntaxHighlighter.registerLanguage(name, grammar)).then(
            () => true,
            () => {
                pendingLoads.delete(language);
                return false;
            },
        );
        pendingLoads.set(language, pending);
    }
    return pending;
}

export { SyntaxHighlighter };
