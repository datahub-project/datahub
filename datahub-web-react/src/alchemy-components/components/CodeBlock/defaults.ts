import { CodeBlockProps } from '@components/components/CodeBlock/types';

export const codeBlockDefaults: Required<
    Pick<CodeBlockProps, 'language' | 'showHeader' | 'showCopy' | 'showLineNumbers' | 'wrap' | 'overflow' | 'variant'>
> = {
    language: 'sql',
    showHeader: true,
    showCopy: true,
    showLineNumbers: false,
    wrap: false,
    overflow: 'auto',
    variant: 'card',
};
