import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { ListChecks } from '@phosphor-icons/react/dist/csr/ListChecks';
import { MagicWand } from '@phosphor-icons/react/dist/csr/MagicWand';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { useTheme } from 'styled-components';

import { Button } from '@components/components/Button';
import { CodeBlockEditor } from '@components/components/CodeBlock/CodeBlockEditor';
import { LanguageControl } from '@components/components/CodeBlock/LanguageControl';
import {
    CodeBlockContainer,
    CodeBlockContent,
    CodeBlockEmptyBody,
    CodeBlockErrorMessage,
    CodeBlockHeader,
    CodeBlockIssueList,
    CodeBlockRoot,
    CodeBlockWarningMessage,
    HeaderLeft,
    HeaderRight,
    LanguageLabel,
    TruncatedBanner,
} from '@components/components/CodeBlock/components';
import { codeBlockDefaults } from '@components/components/CodeBlock/defaults';
import {
    formatCode,
    isFormatCodeSuccess,
    resolveCodeLanguage,
    resolveSqlFormatterLanguage,
} from '@components/components/CodeBlock/formatCode';
import { getCodeBlockPrismStyle } from '@components/components/CodeBlock/prismTheme';
import { CodeBlockProps } from '@components/components/CodeBlock/types';
import {
    getCodeBlockTestId,
    isCodeBlockEditable,
    isCodeBlockHeightCapped,
    mergeHighlightedLineProps,
    resolveCodeBlockBodyMaxHeight,
    resolveCodeBlockStatusDisplay,
    resolveLanguageLabel,
    shouldShowCodeBlockHeader,
    shouldShowFormatButton,
    shouldShowLineNumbers,
    shouldShowValidateButton,
} from '@components/components/CodeBlock/utils';
import { type ValidateCodeSeverity, validateCode } from '@components/components/CodeBlock/validateCode';
import { toast } from '@components/components/Toast';

const COPY_FEEDBACK_MS = 2000;

type SyntaxIssue = {
    severity: ValidateCodeSeverity;
    details: string[];
};

function StatusMessageBody({ message, details }: { message: string; details: string[] }) {
    if (details.length === 0) {
        return <>{message}</>;
    }
    return (
        <>
            <div>{message}</div>
            <CodeBlockIssueList>
                {details.map((detail) => (
                    <li key={detail}>{detail}</li>
                ))}
            </CodeBlockIssueList>
        </>
    );
}

export function CodeBlock({
    code,
    onChange,
    isReadOnly,
    isDisabled,
    placeholder,
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
    showFormat = codeBlockDefaults.showFormat,
    validateSyntax = false,
    error,
    warning,
    isInvalid = false,
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
    const [syntaxIssue, setSyntaxIssue] = useState<SyntaxIssue | null>(null);
    const [isValidating, setIsValidating] = useState(false);
    const copyResetTimeoutRef = useRef<number | null>(null);
    const isMountedRef = useRef(true);
    const validateRequestIdRef = useRef(0);
    const isCopied = isCopiedProp ?? internalCopied;
    const isEditable = isCodeBlockEditable({ isReadOnly, onChange });
    const showFormatButton = shouldShowFormatButton({ showFormat, isEditable: isEditable && !isDisabled });
    const showValidateButton = shouldShowValidateButton({
        validateSyntax,
        isEditable: isEditable && !isDisabled,
    });
    const activeLanguage = selectedLanguage ?? language;
    const highlightLanguage = resolveCodeLanguage(language);
    const prismStyle = getCodeBlockPrismStyle(theme.colors);
    const bodyMaxHeight = resolveCodeBlockBodyMaxHeight({ isEditable, maxHeight });
    const isHeightCapped = isCodeBlockHeightCapped(bodyMaxHeight);
    const isEmptyReadOnly = !isEditable && !code.trim();
    const emptyBody = isEmptyReadOnly ? footer : null;

    const status = resolveCodeBlockStatusDisplay({
        error,
        warning,
        isInvalid,
        syntaxSeverity: syntaxIssue?.severity ?? null,
        syntaxDetails: syntaxIssue?.details ?? [],
        syntaxErrorMessage: t('codeBlock.syntaxInvalid'),
        syntaxWarningMessage: t('codeBlock.sqlSyntaxWarning'),
    });

    useEffect(() => {
        isMountedRef.current = true;
        return () => {
            isMountedRef.current = false;
            if (copyResetTimeoutRef.current !== null) {
                window.clearTimeout(copyResetTimeoutRef.current);
            }
        };
    }, []);

    // Stale findings after edits would be misleading — clear until Validate runs again.
    useEffect(() => {
        validateRequestIdRef.current += 1;
        setSyntaxIssue(null);
        setIsValidating(false);
    }, [activeLanguage, code]);

    const showCodeHeader = shouldShowCodeBlockHeader({
        showHeader,
        showCopy,
        showFormat: showFormatButton,
        showValidate: showValidateButton,
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

    const handleValidate = useCallback(() => {
        if (isDisabled || isValidating || !code.trim()) {
            return;
        }
        const requestId = validateRequestIdRef.current + 1;
        validateRequestIdRef.current = requestId;
        setIsValidating(true);
        validateCode(code, activeLanguage)
            .then((result) => {
                if (!isMountedRef.current || requestId !== validateRequestIdRef.current) {
                    return;
                }
                if (result.valid) {
                    setSyntaxIssue(null);
                    toast.success(t('codeBlock.validateSuccess'));
                    return;
                }
                setSyntaxIssue({
                    severity: result.severity,
                    details: result.details,
                });
            })
            .finally(() => {
                if (isMountedRef.current && requestId === validateRequestIdRef.current) {
                    setIsValidating(false);
                }
            });
    }, [activeLanguage, code, isDisabled, isValidating, t]);

    const handleFormat = useCallback(() => {
        if (!onChange || isDisabled) {
            return;
        }
        const result = formatCode(code, activeLanguage);
        if (isFormatCodeSuccess(result)) {
            onChange(result.formatted);
            setSyntaxIssue(null);
            toast.success(t('codeBlock.formatSuccess'));
            return;
        }
        if (result.error === 'invalid') {
            // Inline only while editing — avoids toast + message double noise.
            const severity: ValidateCodeSeverity = resolveSqlFormatterLanguage(activeLanguage) ? 'warning' : 'error';
            if (validateSyntax) {
                const requestId = validateRequestIdRef.current + 1;
                validateRequestIdRef.current = requestId;
                validateCode(code, activeLanguage).then((validation) => {
                    if (!isMountedRef.current || requestId !== validateRequestIdRef.current) {
                        return;
                    }
                    setSyntaxIssue(
                        validation.valid
                            ? { severity, details: [] }
                            : { severity: validation.severity, details: validation.details },
                    );
                });
            } else {
                setSyntaxIssue({ severity, details: [] });
            }
        }
    }, [activeLanguage, code, isDisabled, onChange, t, validateSyntax]);

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

    let body: React.ReactNode;
    if (isEditable && onChange) {
        body = (
            <CodeBlockEditor
                code={code}
                language={highlightLanguage}
                wrap={wrap}
                isDisabled={isDisabled}
                placeholder={placeholder}
                customStyle={customStyle}
                prismStyle={prismStyle}
                maxHeight={isHeightCapped ? bodyMaxHeight : undefined}
                onChange={onChange}
                onRequestFormat={showFormatButton ? handleFormat : undefined}
                dataTestId={getCodeBlockTestId(dataTestId, 'editor', 'code-block-editor')}
                ariaLabel={t('codeBlock.editorLabel')}
            />
        );
    } else if (emptyBody) {
        body = (
            <CodeBlockEmptyBody data-testid={getCodeBlockTestId(dataTestId, 'empty', 'code-block-empty')}>
                {emptyBody}
            </CodeBlockEmptyBody>
        );
    } else {
        body = (
            <SyntaxHighlighter
                language={highlightLanguage}
                style={prismStyle}
                showLineNumbers={showNumbers}
                lineNumberStyle={lineNumberStyle}
                wrapLongLines={wrap}
                wrapLines={!!highlightedLines || !!linePropsProp}
                lineProps={highlightedLines || linePropsProp ? mergedLineProps : undefined}
                customStyle={customStyle}
            >
                {code}
            </SyntaxHighlighter>
        );
    }

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
        <CodeBlockRoot className={className}>
            <CodeBlockContainer
                $variant={variant}
                $editable={isEditable}
                $isInvalid={status.isInvalid}
                $hasWarning={status.hasWarning}
                data-testid={dataTestId}
                {...rest}
            >
                {showCodeHeader && (
                    <CodeBlockHeader>
                        <HeaderLeft>
                            {languageChrome}
                            {headerLeft}
                        </HeaderLeft>
                        <HeaderRight>
                            {headerRight}
                            {showValidateButton && (
                                <Button
                                    variant="text"
                                    color="gray"
                                    size="sm"
                                    onClick={handleValidate}
                                    isLoading={isValidating}
                                    disabled={isDisabled || !code.trim()}
                                    icon={{ icon: ListChecks }}
                                    iconPosition="left"
                                    data-testid={getCodeBlockTestId(dataTestId, 'validate', 'code-block-validate')}
                                >
                                    {t('codeBlock.validate')}
                                </Button>
                            )}
                            {showFormatButton && (
                                <Button
                                    variant="text"
                                    color="gray"
                                    size="sm"
                                    onClick={handleFormat}
                                    icon={{ icon: MagicWand }}
                                    iconPosition="left"
                                    data-testid={getCodeBlockTestId(dataTestId, 'format', 'code-block-format')}
                                >
                                    {t('codeBlock.format')}
                                </Button>
                            )}
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
                    $overflow={isEditable && isHeightCapped ? 'hidden' : overflow}
                    $maxHeight={isEditable && isHeightCapped ? undefined : bodyMaxHeight}
                    $clickable={!isEditable && !!onCodeClick}
                    $editable={isEditable}
                    onClick={isEditable ? undefined : onCodeClick}
                    data-testid={contentDataTestId ?? getCodeBlockTestId(dataTestId, 'content', 'code-block-content')}
                >
                    {body}
                </CodeBlockContent>
                {isTruncated && (
                    <TruncatedBanner>
                        <Warning size={16} weight="fill" color={theme.colors.iconWarning} />
                        <span>{truncatedMessage ?? t('codeBlock.truncated')}</span>
                    </TruncatedBanner>
                )}
                {emptyBody ? null : footer}
            </CodeBlockContainer>
            {status.displayError ? (
                <CodeBlockErrorMessage data-testid={getCodeBlockTestId(dataTestId, 'error', 'code-block-error')}>
                    <StatusMessageBody message={status.displayError} details={status.statusDetails} />
                </CodeBlockErrorMessage>
            ) : null}
            {status.displayWarning ? (
                <CodeBlockWarningMessage data-testid={getCodeBlockTestId(dataTestId, 'warning', 'code-block-warning')}>
                    <StatusMessageBody message={status.displayWarning} details={status.statusDetails} />
                </CodeBlockWarningMessage>
            ) : null}
        </CodeBlockRoot>
    );
}
