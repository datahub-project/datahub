import React, { Suspense, lazy, useCallback } from 'react';

import { TabButtons } from '@components/components/ButtonTabs/TabButtons';
import { LanguageLabel, LanguageSelectWrapper } from '@components/components/CodeBlock/components';
import { CodeBlockLanguageOption } from '@components/components/CodeBlock/types';
import {
    getLanguageControlMode,
    mapLanguageOptionsToTabItems,
    resolveStaticLanguageLabel,
} from '@components/components/CodeBlock/utils';

// Lazy to avoid circular import: SimpleSelect → @components barrel → CodeBlock
const SimpleSelect = lazy(() =>
    import('@components/components/Select/SimpleSelect').then((mod) => ({ default: mod.SimpleSelect })),
);

type LanguageControlProps = {
    options: CodeBlockLanguageOption[];
    selectedLanguage?: string;
    onLanguageChange?: (value: string) => void;
    staticLabel: string | null;
    ariaLabel: string;
    dataTestId?: string;
};

export function LanguageControl({
    options,
    selectedLanguage,
    onLanguageChange,
    staticLabel,
    ariaLabel,
    dataTestId,
}: LanguageControlProps) {
    const mode = getLanguageControlMode(options, onLanguageChange);

    const handleSelectUpdate = useCallback(
        (values: string[]) => {
            const next = values[0];
            if (next && onLanguageChange) {
                onLanguageChange(next);
            }
        },
        [onLanguageChange],
    );

    if (mode === 'static') {
        const label = resolveStaticLanguageLabel(options, selectedLanguage, staticLabel);
        return label ? <LanguageLabel>{label}</LanguageLabel> : null;
    }

    if (mode === 'tabs' && onLanguageChange) {
        return (
            <TabButtons
                tabs={mapLanguageOptionsToTabItems(options, dataTestId)}
                activeTab={selectedLanguage ?? options[0].value}
                onTabClick={onLanguageChange}
                fit="hug"
                data-testid={dataTestId}
                aria-label={ariaLabel}
            />
        );
    }

    return (
        <LanguageSelectWrapper>
            <Suspense fallback={staticLabel ? <LanguageLabel>{staticLabel}</LanguageLabel> : null}>
                <SimpleSelect
                    options={options}
                    values={selectedLanguage ? [selectedLanguage] : undefined}
                    onUpdate={handleSelectUpdate}
                    size="sm"
                    width="full"
                    showClear={false}
                    dataTestId={dataTestId}
                />
            </Suspense>
        </LanguageSelectWrapper>
    );
}
