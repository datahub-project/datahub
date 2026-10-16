import type { CSSProperties, ComponentType, HTMLAttributes, ReactNode } from 'react';
import bash from 'react-syntax-highlighter/dist/esm/languages/prism/bash';
import graphql from 'react-syntax-highlighter/dist/esm/languages/prism/graphql';
import json from 'react-syntax-highlighter/dist/esm/languages/prism/json';
import markdown from 'react-syntax-highlighter/dist/esm/languages/prism/markdown';
import protobuf from 'react-syntax-highlighter/dist/esm/languages/prism/protobuf';
import python from 'react-syntax-highlighter/dist/esm/languages/prism/python';
import sql from 'react-syntax-highlighter/dist/esm/languages/prism/sql';
import typescript from 'react-syntax-highlighter/dist/esm/languages/prism/typescript';
import yaml from 'react-syntax-highlighter/dist/esm/languages/prism/yaml';
// The package entry re-exports full Prism, which registers every refractor
// grammar (~185 KB gzip) into whichever chunk imports it. prism-light is
// untyped JS and only loads refractor's core. The grammars below are the ones
// CodeBlock shows. refractor/core is that same instance, so aliases land on it.
import prismLight from 'react-syntax-highlighter/dist/esm/prism-light';
import refractor from 'refractor/core';

import { CODE_BLOCK_LANGUAGE_ALIASES } from '@components/components/CodeBlock/codeBlockLanguages';

type PrismLightProps = {
    language?: string;
    style?: { [key: string]: CSSProperties };
    showLineNumbers?: boolean;
    lineNumberStyle?: CSSProperties;
    wrapLongLines?: boolean;
    wrapLines?: boolean;
    lineProps?: (lineNumber: number) => HTMLAttributes<HTMLElement>;
    customStyle?: CSSProperties;
    children?: ReactNode;
    className?: string;
};

type PrismLightComponent = ComponentType<PrismLightProps> & {
    registerLanguage: (name: string, language: unknown) => void;
};

const SyntaxHighlighter = prismLight as unknown as PrismLightComponent;

const GRAMMARS: Record<string, unknown> = {
    bash,
    graphql,
    json,
    markdown,
    protobuf,
    python,
    sql,
    typescript,
    yaml,
};

Object.entries(GRAMMARS).forEach(([name, grammar]) => {
    SyntaxHighlighter.registerLanguage(name, grammar);
});

Object.entries(CODE_BLOCK_LANGUAGE_ALIASES).forEach(([name, aliasNames]) => {
    refractor.alias(name, [...aliasNames]);
});

export { SyntaxHighlighter };
