import { CodeBlockProps } from '@components/components/CodeBlock/types';

export const CODE_BLOCK_TAB_INDENT = '  ';

/** Default body cap for the writable editor so long pastes scroll inside the block. */
export const CODE_BLOCK_EDITOR_DEFAULT_MAX_HEIGHT = 400;

export const codeBlockDefaults: Required<
    Pick<
        CodeBlockProps,
        'language' | 'showHeader' | 'showCopy' | 'showFormat' | 'showLineNumbers' | 'wrap' | 'overflow' | 'variant'
    >
> = {
    language: 'sql',
    showHeader: true,
    showCopy: true,
    showFormat: true,
    showLineNumbers: false,
    wrap: false,
    overflow: 'auto',
    variant: 'card',
};
