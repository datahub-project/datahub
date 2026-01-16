import { useState } from 'react';
import ReactMarkdown from 'react-markdown';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';

interface MarkdownRendererProps {
  content: string;
  className?: string;
}

export function MarkdownRenderer({ content, className = '' }: MarkdownRendererProps) {
  return (
    <div className={`markdown-renderer ${className}`}>
      <ReactMarkdown
        components={{
          code(props) {
            const { node, inline, className, children, ...rest } = props as any;
            const match = /language-(\w+)/.exec(className || '');
            const language = match ? match[1] : '';

            return !inline && language ? (
              <CodeBlock
                language={language}
                code={String(children).replace(/\n$/, '')}
              />
            ) : (
              <code className={className} {...rest}>
                {children}
              </code>
            );
          },
        }}
      >
        {content}
      </ReactMarkdown>
    </div>
  );
}

interface CodeBlockProps {
  language: string;
  code: string;
}

function CodeBlock({ language, code }: CodeBlockProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="code-block">
      <div className="code-block-header">
        <span className="code-block-language">{language.toUpperCase()}</span>
        <button
          className="code-block-copy-btn"
          onClick={handleCopy}
          aria-label="Copy code"
        >
          {copied ? '✓ Copied' : '📋 Copy'}
        </button>
      </div>
      <SyntaxHighlighter
        language={language}
        style={vscDarkPlus}
        customStyle={{
          margin: 0,
          borderRadius: '0 0 4px 4px',
          fontSize: '0.9em',
        }}
      >
        {code}
      </SyntaxHighlighter>
    </div>
  );
}
