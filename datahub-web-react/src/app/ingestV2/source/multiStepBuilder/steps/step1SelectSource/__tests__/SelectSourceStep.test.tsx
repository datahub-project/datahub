import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { SelectSourceStep } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SelectSourceStep';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

vi.mock('@app/sharedV2/forms/multiStepForm/MultiStepFormContext', () => ({
    useMultiStepContext: () => ({
        updateState: vi.fn(),
        setCurrentStepCompleted: vi.fn(),
        isCurrentStepCompleted: () => false,
        goToNext: vi.fn(),
    }),
}));
// Like the real hook: one column on the first render, then the measured width.
vi.mock('@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/useCardsPerRow', async () => {
    const { useEffect, useState } = await import('react');
    return {
        useCardsPerRow: () => {
            const [cardsPerRow, setCardsPerRow] = useState(1);
            useEffect(() => setCardsPerRow(10), []);
            return cardsPerRow;
        },
    };
});
vi.mock('@app/ingestV2/source/multiStepBuilder/CreateSourceEducationModal', () => ({ default: () => null }));

describe('SelectSourceStep', () => {
    // dbt and dbt Cloud (like Hive and Hive Metastore) share a platform URN, so cards keyed by URN collide.
    it('renders each source once when tiles share a platform URN', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
        render(
            <MockedProvider>
                <TestPageContainer>
                    <SelectSourceStep />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getAllByText('dbt', { exact: true })).toHaveLength(1);
        expect(screen.getAllByText('dbt Cloud', { exact: true })).toHaveLength(1);
        // Duplicate keys let React reuse one card for another (dbt rendered twice in the browser).
        const duplicateKeyWarnings = consoleError.mock.calls.filter((args) =>
            String(args[0]).includes('Encountered two children with the same key'),
        );
        expect(duplicateKeyWarnings).toEqual([]);
        consoleError.mockRestore();
    });
});
