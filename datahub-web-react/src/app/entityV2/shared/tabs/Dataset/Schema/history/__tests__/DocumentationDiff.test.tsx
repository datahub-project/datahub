import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { vi } from 'vitest';

import DocumentationDiff from '@app/entityV2/shared/tabs/Dataset/Schema/history/DocumentationDiff';
import { ChangeCategoryType, ChangeEvent, ChangeOperationType, TimelineParameterEntry } from '@src/types.generated';

// Mock react-diff-viewer to avoid its heavy rendering in unit tests
vi.mock('react-diff-viewer', () => ({
    default: ({ oldValue, newValue }: { oldValue: string; newValue: string }) => (
        <div data-testid="mock-diff-viewer" data-old={oldValue} data-new={newValue} />
    ),
    DiffMethod: { LINES: 'LINES' },
}));

// Mock react-i18next
vi.mock('react-i18next', () => ({
    useTranslation: () => ({
        t: (key: string) => key,
    }),
}));

vi.mock('@src/alchemy-components', () => ({
    colors: {
        green: { 1300: '#b4f0b4', 1000: '#22863a' },
        red: { 100: '#ffc9c9', 1000: '#c0392b' },
    },
}));

const testTheme = {
    colors: {
        border: '#e0e0e0',
        hyperlinks: '#1890ff',
        textSecondary: '#888',
    },
};

function Wrapper({ children }: { children: React.ReactNode }) {
    return <ThemeProvider theme={testTheme as any}>{children}</ThemeProvider>;
}

function makeChangeEvent(
    operation: ChangeOperationType,
    description?: string,
    previousDescription?: string,
): ChangeEvent {
    const parameters: TimelineParameterEntry[] = [];
    if (description !== undefined) parameters.push({ key: 'description', value: description });
    if (previousDescription !== undefined) parameters.push({ key: 'previousDescription', value: previousDescription });

    return {
        urn: 'urn:test',
        category: ChangeCategoryType.Documentation,
        operation,
        parameters,
        description: 'Documentation changed',
    };
}

describe('DocumentationDiff', () => {
    it('renders summary and toggle link for a MODIFY event', () => {
        const event = makeChangeEvent(ChangeOperationType.Modify, 'New text', 'Old text');
        render(<DocumentationDiff changeEvent={event} />, { wrapper: Wrapper });

        expect(screen.getByText('documentationUpdated')).toBeInTheDocument();
        expect(screen.getByText('showDiff')).toBeInTheDocument();
    });

    it('renders summary for an ADD event', () => {
        const event = makeChangeEvent(ChangeOperationType.Add, 'Added text');
        render(<DocumentationDiff changeEvent={event} />, { wrapper: Wrapper });

        expect(screen.getByText('documentationAdded')).toBeInTheDocument();
    });

    it('renders summary for a REMOVE event', () => {
        const event = makeChangeEvent(ChangeOperationType.Remove, undefined, 'Removed text');
        render(<DocumentationDiff changeEvent={event} />, { wrapper: Wrapper });

        expect(screen.getByText('documentationRemoved')).toBeInTheDocument();
    });

    it('expands diff when show diff is clicked', () => {
        const event = makeChangeEvent(ChangeOperationType.Modify, 'New text', 'Old text');
        render(<DocumentationDiff changeEvent={event} />, { wrapper: Wrapper });

        expect(screen.queryByTestId('documentation-diff-content')).not.toBeInTheDocument();
        fireEvent.click(screen.getByText('showDiff'));
        expect(screen.getByTestId('documentation-diff-content')).toBeInTheDocument();
    });

    it('collapses diff when hide diff is clicked', () => {
        const event = makeChangeEvent(ChangeOperationType.Modify, 'New text', 'Old text');
        render(<DocumentationDiff changeEvent={event} />, { wrapper: Wrapper });

        fireEvent.click(screen.getByText('showDiff'));
        expect(screen.getByTestId('documentation-diff-content')).toBeInTheDocument();

        fireEvent.click(screen.getByText('hideDiff'));
        expect(screen.queryByTestId('documentation-diff-content')).not.toBeInTheDocument();
    });

    it('passes old and new values to diff viewer for MODIFY', () => {
        const event = makeChangeEvent(ChangeOperationType.Modify, 'New text', 'Old text');
        render(<DocumentationDiff changeEvent={event} />, { wrapper: Wrapper });

        fireEvent.click(screen.getByText('showDiff'));
        const viewer = screen.getByTestId('mock-diff-viewer');
        expect(viewer.getAttribute('data-old')).toBe('Old text');
        expect(viewer.getAttribute('data-new')).toBe('New text');
    });

    it('strips code fences before diffing', () => {
        const event = makeChangeEvent(
            ChangeOperationType.Modify,
            '```yaml\nname: test\n```',
            '```yaml\nname: old\n```',
        );
        render(<DocumentationDiff changeEvent={event} />, { wrapper: Wrapper });

        fireEvent.click(screen.getByText('showDiff'));
        const viewer = screen.getByTestId('mock-diff-viewer');
        expect(viewer.getAttribute('data-old')).toBe('name: old');
        expect(viewer.getAttribute('data-new')).toBe('name: test');
    });

    it('uses inherited previous description for ADD in all-versions mode', () => {
        const event = makeChangeEvent(ChangeOperationType.Add, 'New text');
        render(<DocumentationDiff changeEvent={event} inheritedPreviousDescription="Prior text" />, {
            wrapper: Wrapper,
        });

        fireEvent.click(screen.getByText('showDiff'));
        const viewer = screen.getByTestId('mock-diff-viewer');
        expect(viewer.getAttribute('data-old')).toBe('Prior text');
        expect(viewer.getAttribute('data-new')).toBe('New text');
    });

    it('returns null when hasDiff is false', () => {
        // Modify with no prevText — should not render
        const event = makeChangeEvent(ChangeOperationType.Modify, 'New text');
        const { container } = render(<DocumentationDiff changeEvent={event} />, { wrapper: Wrapper });
        expect(container.firstChild).toBeNull();
    });
});
