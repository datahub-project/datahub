import { Button, colors } from '@components';
import { Warning } from '@phosphor-icons/react';
import MDEditor from '@uiw/react-md-editor';
import React from 'react';
import styled from 'styled-components';

// Shared style object for MDEditor.Markdown (avoids recreating on every render)
const MARKDOWN_STYLE = { backgroundColor: 'transparent', color: 'inherit' };

/* Code Block Container - matches Figma design */
const CodeBlockContainer = styled.div`
    display: flex;
    flex-direction: column;
    background: ${colors.white};
    border: 1px solid ${colors.gray[100]};
    box-shadow: 0px 4px 8px rgba(33, 23, 95, 0.04);
    border-radius: 12px;
    margin: 8px 0 24px 0;
    overflow: hidden;
    width: 100%;
`;

const CodeBlockHeader = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    height: 40px;
    background: ${colors.gray[1500]};
    border-bottom: 1px solid ${colors.gray[100]};
`;

const CodeBlockLanguageLabel = styled.span`
    font-family: 'Mulish', sans-serif;
    font-weight: 700;
    font-size: 12px;
    line-height: 15px;
    color: ${colors.gray[600]};
`;

const CodeBlockContent = styled.div`
    overflow-x: auto;

    & pre {
        margin: 0 !important;
        padding: 0 !important;
        background: transparent !important;
        border: none !important;
        border-radius: 0 !important;
        overflow: visible !important;
    }

    & code {
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 14px;
        line-height: 18px;
    }
`;

const TruncatedBanner = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 8px;
    margin: 12px 16px;
    background: ${colors.yellow[0]};
    border-radius: 6px;
    font-family: 'Mulish', sans-serif;
    font-size: 14px;
    line-height: 20px;
    color: ${colors.yellow[1000]};
`;

export interface CodeBlockProps {
    /** The programming language of the code block (e.g., 'sql', 'javascript', 'python') */
    language: string;
    /** The code content to display */
    content: string;
    /** Whether this code block is truncated (incomplete due to max length) */
    isTruncated?: boolean;
    /** Whether the code has been copied to clipboard */
    isCopied?: boolean;
    /** Callback when the copy button is clicked */
    onCopy?: () => void;
}

/**
 * Extracted component to encapsulate code block rendering and styling.
 *
 * Rationale:
 * - Separating code blocks from markdown allows custom styling (headers, copy buttons)
 *   that would be difficult to achieve with MDEditor's default rendering
 * - Centralized component improves maintainability and ensures consistent code block
 *   presentation across all chat messages
 * - Enables special handling for truncated code blocks with warning UI
 * - Provides better accessibility with clear language labels and copy functionality
 *
 * @example
 * <CodeBlock
 *   language="sql"
 *   content="SELECT * FROM users"
 *   isCopied={false}
 *   onCopy={() => handleCopy()}
 * />
 */
export const CodeBlock: React.FC<CodeBlockProps> = ({ language, content, isTruncated, isCopied, onCopy }) => {
    return (
        <CodeBlockContainer>
            <CodeBlockHeader>
                <CodeBlockLanguageLabel>{language.toUpperCase()}</CodeBlockLanguageLabel>
                <Button
                    variant="text"
                    color="gray"
                    size="sm"
                    onClick={onCopy}
                    icon={isCopied ? { icon: 'Check', source: 'phosphor' } : { icon: 'Copy', source: 'phosphor' }}
                    iconPosition="left"
                >
                    {isCopied ? 'Copied' : 'Copy'}
                </Button>
            </CodeBlockHeader>
            <CodeBlockContent>
                <MDEditor.Markdown source={`\`\`\`${language}\n${content}\n\`\`\``} style={MARKDOWN_STYLE} />
            </CodeBlockContent>
            {isTruncated && (
                <TruncatedBanner>
                    <Warning size={16} weight="fill" color={colors.yellow[500]} />
                    <span>The query exceeds the maximum length. Try generating something shorter.</span>
                </TruncatedBanner>
            )}
        </CodeBlockContainer>
    );
};
