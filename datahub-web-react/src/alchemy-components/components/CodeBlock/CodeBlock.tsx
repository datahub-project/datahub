import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { ghcolors } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { useTheme } from 'styled-components';

import { Button } from '@components/components/Button';
import { LanguageControl } from '@components/components/CodeBlock/LanguageControl';
import {
    CodeBlockContainer,
    CodeBlockContent,
    CodeBlockHeader,
    HeaderLeft,
    HeaderRight,
    LanguageLabel,
    TruncatedBanner,
} from '@components/components/CodeBlock/components';
import { codeBlockDefaults } from '@components/components/CodeBlock/defaults';
import { CodeBlockProps } from '@components/components/CodeBlock/types';
import {
    getCodeBlockTestId,
    mergeHighlightedLineProps,
    resolveLanguageLabel,
    shouldShowCodeBlockHeader,
    shouldShowLineNumbers,
} from '@components/components/CodeBlock/utils';
import { toast } from '@components/components/Toast';

const COPY_FEEDBACK_MS = 2000;

export function CodeBlock({
    code,
    language = codeBlockDefaults.language,
    languageLabel,
    languageOptions,
    selectedLanguage,
    onLanguageChange,
    headerLeft,
    headerRight,
    footer,
    showHeader = codeBlockDefaults.showHeader,
    showCopy = codeBlockDefaults.showCopy,
    copyText,
    isCopied: isCopiedProp,
    onCopy,
    isTruncated = false,
    truncatedMessage,
    showLineNumbers = codeBlockDefaults.showLineNumbers,
    hideLineNumbers = false,
    wrap = codeBlockDefaults.wrap,
    highlightedLines,
    lineProps: linePropsProp,
    maxHeight,
    overflow = codeBlockDefaults.overflow,
    variant = codeBlockDefaults.variant,
    customStyle,
    onCodeClick,
    copyDataTestId,
    contentDataTestId,
    className,
    'data-testid': dataTestId,
    ...rest
}: CodeBlockProps) {
    const { t } = useTranslation('alchemy');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const theme = useTheme();
    const [internalCopied, setInternalCopied] = useState(false);
    const copyResetTimeoutRef = useRef<number | null>(null);
    const isMountedRef = useRef(true);
    const isCopied = isCopiedProp ?? internalCopied;

    useEffect(() => {
        isMountedRef.current = true;
        return () => {
            isMountedRef.current = false;
            if (copyResetTimeoutRef.current !== null) {
                window.clearTimeout(copyResetTimeoutRef.current);
            }
        };
    }, []);

    const showCodeHeader = shouldShowCodeBlockHeader({
        showHeader,
        showCopy,
        languageOptions,
        headerLeft,
        headerRight,
        languageLabel,
    });

    const resolvedLanguageLabel = resolveLanguageLabel({
        language,
        languageLabel,
        languageOptions,
        selectedLanguage,
        hasHeaderLeft: !!headerLeft,
    });

    const handleCopy = useCallback(() => {
        const text = copyText ?? code;
        navigator.clipboard.writeText(text).then(
            () => {
                if (!isMountedRef.current) {
                    return;
                }
                if (isCopiedProp === undefined) {
                    if (copyResetTimeoutRef.current !== null) {
                        window.clearTimeout(copyResetTimeoutRef.current);
                    }
                    setInternalCopied(true);
                    copyResetTimeoutRef.current = window.setTimeout(() => {
                        if (isMountedRef.current) {
                            setInternalCopied(false);
                        }
                        copyResetTimeoutRef.current = null;
                    }, COPY_FEEDBACK_MS);
                }
                toast.success(tf('copiedSuccess'));
                onCopy?.();
            },
            () => {
                if (isMountedRef.current) {
                    toast.error(tf('somethingWentWrong'));
                }
            },
        );
    }, [code, copyText, isCopiedProp, onCopy, tf]);

    const mergedLineProps = useCallback(
        (lineNumber: number) =>
            mergeHighlightedLineProps(lineNumber, highlightedLines, theme.colors.bgHover, linePropsProp),
        [highlightedLines, linePropsProp, theme.colors.bgHover],
    );

    const lineNumberStyle = hideLineNumbers ? { display: 'none' } : undefined;
    const showNumbers = shouldShowLineNumbers({ showLineNumbers, hideLineNumbers, highlightedLines });

    let languageChrome: React.ReactNode = null;
    if (languageOptions?.length) {
        languageChrome = (
            <LanguageControl
                options={languageOptions}
                selectedLanguage={selectedLanguage}
                onLanguageChange={onLanguageChange}
                staticLabel={resolvedLanguageLabel}
                ariaLabel={t('codeBlock.languageSelectLabel')}
                dataTestId={getCodeBlockTestId(dataTestId, 'language', 'code-block-language')}
            />
        );
    } else if (resolvedLanguageLabel) {
        languageChrome = <LanguageLabel>{resolvedLanguageLabel}</LanguageLabel>;
    }

    return (
        <CodeBlockContainer $variant={variant} className={className} data-testid={dataTestId} {...rest}>
            {showCodeHeader && (
                <CodeBlockHeader>
                    <HeaderLeft>
                        {languageChrome}
                        {headerLeft}
                    </HeaderLeft>
                    <HeaderRight>
                        {headerRight}
                        {showCopy && (
                            <Button
                                variant="text"
                                color="gray"
                                size="sm"
                                onClick={handleCopy}
                                icon={isCopied ? { icon: Check } : { icon: Copy }}
                                iconPosition="left"
                                data-testid={
                                    copyDataTestId ?? getCodeBlockTestId(dataTestId, 'copy', 'code-block-copy')
                                }
                            >
                                {isCopied ? tf('copied') : tc('copy')}
                            </Button>
                        )}
                    </HeaderRight>
                </CodeBlockHeader>
            )}
            <CodeBlockContent
                $variant={variant}
                $overflow={overflow}
                $maxHeight={maxHeight}
                $clickable={!!onCodeClick}
                onClick={onCodeClick}
                data-testid={contentDataTestId ?? getCodeBlockTestId(dataTestId, 'content', 'code-block-content')}
            >
                <SyntaxHighlighter
                    language={language}
                    style={ghcolors}
                    showLineNumbers={showNumbers}
                    lineNumberStyle={lineNumberStyle}
                    wrapLongLines={wrap}
                    wrapLines={!!highlightedLines || !!linePropsProp}
                    lineProps={highlightedLines || linePropsProp ? mergedLineProps : undefined}
                    customStyle={customStyle}
                >
                    {code}
                </SyntaxHighlighter>
            </CodeBlockContent>
            {isTruncated && (
                <TruncatedBanner>
                    <Warning size={16} weight="fill" color={theme.colors.iconWarning} />
                    <span>{truncatedMessage ?? t('codeBlock.truncated')}</span>
                </TruncatedBanner>
            )}
            {footer}
        </CodeBlockContainer>
    );
}
