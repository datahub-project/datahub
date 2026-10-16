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
import refractor from 'refractor/core';

import { CODE_BLOCK_LANGUAGE_ALIASES } from '@components/components/CodeBlock/codeBlockLanguages';
import type { PrismLightComponent } from '@components/components/CodeBlock/prismLightTypes';

// allowJs binds prism-light.js, so the ambient module does not type this
// import. registerLanguage ignores `name` and uses grammar.displayName.
const SyntaxHighlighter = prismLight as unknown as PrismLightComponent;

// prism-light only loads refractor's core. These grammars are the ones the
// standalone code block shows. The docs editor registers the full catalog.
const GRAMMARS = [bash, graphql, json, markdown, protobuf, python, sql, typescript, yaml];

GRAMMARS.forEach((grammar) => {
    SyntaxHighlighter.registerLanguage(grammar.displayName, grammar);
});

Object.entries(CODE_BLOCK_LANGUAGE_ALIASES).forEach(([name, aliasNames]) => {
    refractor.alias(name, [...aliasNames]);
});

export { SyntaxHighlighter };
