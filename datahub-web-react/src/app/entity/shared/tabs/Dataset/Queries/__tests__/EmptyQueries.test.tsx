import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import EmptyQueries from '@app/entity/shared/tabs/Dataset/Queries/EmptyQueries';

vi.mock('@app/entity/shared/components/styled/EmptyTab', () => ({
    EmptyTab: ({ children, tab }: { children: React.ReactNode; tab: string }) => (
        <div data-testid={`empty-tab-${tab}`}>{children}</div>
    ),
}));

describe('EmptyQueries', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should render emptyText when provided', () => {
        render(<EmptyQueries emptyText="You don't have permission to view queries." />);

        expect(screen.getByText("You don't have permission to view queries.")).toBeInTheDocument();
        expect(screen.queryByTestId('empty-tab-queries')).not.toBeInTheDocument();
    });

    it('should render EmptyTab with Add Query button when not readOnly', () => {
        const onClickAddQuery = vi.fn();
        render(<EmptyQueries onClickAddQuery={onClickAddQuery} />);

        expect(screen.getByTestId('empty-tab-queries')).toBeInTheDocument();
        const button = screen.getByRole('button', { name: /add query/i });
        expect(button).toBeInTheDocument();

        fireEvent.click(button);
        expect(onClickAddQuery).toHaveBeenCalledTimes(1);
    });

    it('should hide Add Query button when readOnly is true', () => {
        const onClickAddQuery = vi.fn();
        render(<EmptyQueries readOnly onClickAddQuery={onClickAddQuery} />);

        expect(screen.queryByRole('button', { name: /add query/i })).not.toBeInTheDocument();
    });

    it('should hide Add Query button when message is provided', () => {
        const onClickAddQuery = vi.fn();
        render(<EmptyQueries message="Custom message" onClickAddQuery={onClickAddQuery} />);

        expect(screen.queryByRole('button', { name: /add query/i })).not.toBeInTheDocument();
    });

    it('should hide Add Query button when hideButton is true', () => {
        const onClickAddQuery = vi.fn();
        render(<EmptyQueries hideButton onClickAddQuery={onClickAddQuery} />);

        expect(screen.queryByRole('button', { name: /add query/i })).not.toBeInTheDocument();
    });

    it('should hide Add Query button when onClickAddQuery is not provided', () => {
        render(<EmptyQueries />);

        expect(screen.queryByRole('button', { name: /add query/i })).not.toBeInTheDocument();
    });
});
