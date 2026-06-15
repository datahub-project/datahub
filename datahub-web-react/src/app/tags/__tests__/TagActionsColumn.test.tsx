import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { TagActionsColumn } from '@app/tags/TagsTableColumns';
import themeV2 from '@conf/theme/themeV2';

// Mock Apollo hooks so no ApolloProvider is needed
vi.mock('@src/graphql/tag.generated', () => ({
    useGetTagQuery: vi.fn(() => ({
        data: { tag: { deprecation: { deprecated: false } } },
        refetch: vi.fn(),
    })),
}));

vi.mock('@graphql/mutations.generated', () => ({
    useBatchUpdateDeprecationMutation: vi.fn(() => [vi.fn()]),
}));

vi.mock('@components', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@components')>();
    return {
        ...actual,
        Icon: React.forwardRef(({ 'data-testid': testId, ...props }: any, ref: any) => (
            <div ref={ref} data-testid={testId} {...props} />
        )),
        Menu: ({ items, children }: any) => (
            <div>
                {children}
                {items?.map((item: any) => (
                    <button
                        key={item.key}
                        data-testid={item['data-testid'] || item.key}
                        onClick={item.onClick}
                        type="button"
                    >
                        {item.title}
                    </button>
                ))}
            </div>
        ),
        Text: (props: any) => <p {...props} />,
    };
});


const TAG_URN = 'urn:li:tag:TestTag';

const defaultProps = {
    tagUrn: TAG_URN,
    onEdit: vi.fn(),
    onDelete: vi.fn(),
    onDeprecate: vi.fn(),
    canManageTags: true,
};

const renderWithTheme = (ui: React.ReactElement) => render(<ThemeProvider theme={themeV2}>{ui}</ThemeProvider>);

describe('TagActionsColumn', () => {
    it('shows edit, copy URN, deprecate, and delete when canManageTags is true', () => {
        renderWithTheme(<TagActionsColumn {...defaultProps} canManageTags />);

        expect(screen.getByTestId('action-edit')).toBeInTheDocument();
        expect(screen.getByTestId('copy-urn')).toBeInTheDocument();
        expect(screen.getByTestId('action-deprecate')).toBeInTheDocument();
        expect(screen.getByTestId('action-delete')).toBeInTheDocument();
    });

    it('hides deprecate and delete when canManageTags is false', () => {
        renderWithTheme(<TagActionsColumn {...defaultProps} canManageTags={false} />);

        expect(screen.getByTestId('action-edit')).toBeInTheDocument();
        expect(screen.getByTestId('copy-urn')).toBeInTheDocument();
        expect(screen.queryByTestId('action-deprecate')).not.toBeInTheDocument();
        expect(screen.queryByTestId('action-delete')).not.toBeInTheDocument();
    });

    it('calls onEdit when the edit item is clicked', () => {
        const onEdit = vi.fn();
        renderWithTheme(<TagActionsColumn {...defaultProps} onEdit={onEdit} />);

        fireEvent.click(screen.getByTestId('action-edit'));
        expect(onEdit).toHaveBeenCalledTimes(1);
    });

    it('calls onDeprecate when the deprecate item is clicked', () => {
        const onDeprecate = vi.fn();
        renderWithTheme(<TagActionsColumn {...defaultProps} onDeprecate={onDeprecate} />);

        fireEvent.click(screen.getByTestId('action-deprecate'));
        expect(onDeprecate).toHaveBeenCalledTimes(1);
    });

    it('calls onDelete when the delete item is clicked', () => {
        const onDelete = vi.fn();
        renderWithTheme(<TagActionsColumn {...defaultProps} onDelete={onDelete} />);

        fireEvent.click(screen.getByTestId('action-delete'));
        expect(onDelete).toHaveBeenCalledTimes(1);
    });
});
