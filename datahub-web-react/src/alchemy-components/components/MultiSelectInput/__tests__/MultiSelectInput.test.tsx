import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { DefaultTheme, ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { MultiSelectInput } from '@components/components/MultiSelectInput/MultiSelectInput';
import theme from '@components/theme';

const renderWithTheme = (component: React.ReactElement) => {
    return render(<ThemeProvider theme={theme as unknown as DefaultTheme}>{component}</ThemeProvider>);
};

describe('MultiSelectInput', () => {
    const mockOnUpdate = vi.fn();

    beforeEach(() => {
        mockOnUpdate.mockClear();
    });

    it('should render with label', () => {
        renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} label="Test Label" placeholder="Enter tags" />,
        );

        expect(screen.getByText('Test Label')).toBeInTheDocument();
    });

    it('should render without label when not provided', () => {
        renderWithTheme(<MultiSelectInput values={[]} onUpdate={mockOnUpdate} placeholder="Enter tags" />);

        expect(screen.queryByText('Test Label')).not.toBeInTheDocument();
    });

    it('should show placeholder when no values', () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        expect(input.placeholder).toBe('Enter tags');
    });

    it('should hide placeholder when values exist', () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={['tag1']} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        expect(input.placeholder).toBe('');
    });

    it('should add tag on Enter key', async () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        await userEvent.type(input, 'newtag');
        fireEvent.keyDown(input, { key: 'Enter' });

        expect(mockOnUpdate).toHaveBeenCalledWith(['newtag']);
    });

    it('should add tag on comma key', async () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        await userEvent.type(input, 'newtag,');

        expect(mockOnUpdate).toHaveBeenCalledWith(['newtag']);
    });

    it('should trim whitespace from tags', async () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        await userEvent.type(input, '  newtag  ');
        fireEvent.keyDown(input, { key: 'Enter' });

        expect(mockOnUpdate).toHaveBeenCalledWith(['newtag']);
    });

    it('should not add duplicate tags', async () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={['tag1']} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        await userEvent.type(input, 'tag1');
        fireEvent.keyDown(input, { key: 'Enter' });

        expect(mockOnUpdate).not.toHaveBeenCalled();
    });

    it('should not add empty tags', async () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        fireEvent.keyDown(input, { key: 'Enter' });

        expect(mockOnUpdate).not.toHaveBeenCalled();
    });

    it('should clear input after adding tag', async () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        await userEvent.type(input, 'newtag');
        fireEvent.keyDown(input, { key: 'Enter' });

        expect(input.value).toBe('');
    });

    it('should remove tag on delete button click', () => {
        renderWithTheme(
            <MultiSelectInput values={['tag1', 'tag2']} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const removeButton = screen.getByTestId('remove-tag-tag1');
        fireEvent.click(removeButton);

        expect(mockOnUpdate).toHaveBeenCalledWith(['tag2']);
    });

    it('should remove last tag on Backspace when input is empty', async () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={['tag1', 'tag2']} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        fireEvent.keyDown(input, { key: 'Backspace' });

        expect(mockOnUpdate).toHaveBeenCalledWith(['tag1']);
    });

    it('should not remove tag on Backspace when input has value', async () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={['tag1']} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        await userEvent.type(input, 'text');
        fireEvent.keyDown(input, { key: 'Backspace' });

        expect(mockOnUpdate).not.toHaveBeenCalled();
    });

    it('should display all pills for values', () => {
        renderWithTheme(
            <MultiSelectInput values={['tag1', 'tag2', 'tag3']} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        expect(screen.getByTestId('pill-tag1')).toBeInTheDocument();
        expect(screen.getByTestId('pill-tag2')).toBeInTheDocument();
        expect(screen.getByTestId('pill-tag3')).toBeInTheDocument();
    });

    it('should show clear all button when tags exist', () => {
        renderWithTheme(<MultiSelectInput values={['tag1']} onUpdate={mockOnUpdate} placeholder="Enter tags" />);

        expect(screen.getByTestId('clear-all-button')).toBeInTheDocument();
    });

    it('should not show clear all button when no tags', () => {
        renderWithTheme(<MultiSelectInput values={[]} onUpdate={mockOnUpdate} placeholder="Enter tags" />);

        expect(screen.queryByTestId('clear-all-button')).not.toBeInTheDocument();
    });

    it('should clear all tags on clear button click', () => {
        renderWithTheme(
            <MultiSelectInput values={['tag1', 'tag2']} onUpdate={mockOnUpdate} placeholder="Enter tags" />,
        );

        const clearButton = screen.getByTestId('clear-all-button');
        fireEvent.click(clearButton);

        expect(mockOnUpdate).toHaveBeenCalledWith([]);
    });

    it('should display error message when error provided', () => {
        renderWithTheme(<MultiSelectInput values={[]} onUpdate={mockOnUpdate} error="This field is required" />);

        expect(screen.getByText('This field is required')).toBeInTheDocument();
    });

    it('should display helper text when no error', () => {
        renderWithTheme(<MultiSelectInput values={[]} onUpdate={mockOnUpdate} helperText="Enter at least one tag" />);

        expect(screen.getByText('Enter at least one tag')).toBeInTheDocument();
    });

    it('should not display helper text when error exists', () => {
        renderWithTheme(
            <MultiSelectInput
                values={[]}
                onUpdate={mockOnUpdate}
                error="This field is required"
                helperText="Enter at least one tag"
            />,
        );

        expect(screen.queryByText('Enter at least one tag')).not.toBeInTheDocument();
        expect(screen.getByText('This field is required')).toBeInTheDocument();
    });

    it('should disable input when disabled prop is true', () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} disabled placeholder="Enter tags" />,
        );

        const input = container.querySelector('input') as HTMLInputElement;
        expect(input.disabled).toBe(true);
    });

    it('should disable clear button when disabled prop is true', () => {
        renderWithTheme(<MultiSelectInput values={['tag1']} onUpdate={mockOnUpdate} disabled />);

        const clearButton = screen.getByTestId('clear-all-button') as HTMLButtonElement;
        expect(clearButton.disabled).toBe(true);
    });

    it('should apply custom id', () => {
        const { container } = renderWithTheme(<MultiSelectInput values={[]} onUpdate={mockOnUpdate} id="custom-id" />);

        expect(container.querySelector('#custom-id')).toBeInTheDocument();
    });

    it('should apply custom className', () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} className="custom-class" />,
        );

        expect(container.querySelector('.custom-class')).toBeInTheDocument();
    });

    it('should use custom input testid', () => {
        const { container } = renderWithTheme(
            <MultiSelectInput values={[]} onUpdate={mockOnUpdate} inputTestId="custom-input-testid" />,
        );

        const input = container.querySelector('[data-testid="custom-input-testid"]');
        expect(input).toBeInTheDocument();
    });

    it('should support custom width prop with number', () => {
        renderWithTheme(<MultiSelectInput values={[]} onUpdate={mockOnUpdate} width={400} />);

        // Verify component renders without error
        expect(screen.getByRole('textbox')).toBeInTheDocument();
    });

    it('should support custom width prop with string value', () => {
        renderWithTheme(<MultiSelectInput values={[]} onUpdate={mockOnUpdate} width="100%" />);

        // Verify component renders without error
        expect(screen.getByRole('textbox')).toBeInTheDocument();
    });

    it('should default width to 300px', () => {
        renderWithTheme(<MultiSelectInput values={[]} onUpdate={mockOnUpdate} />);

        // Verify component renders without error
        expect(screen.getByRole('textbox')).toBeInTheDocument();
    });

    it('should have accessible aria-label on clear button', () => {
        renderWithTheme(<MultiSelectInput values={['tag1']} onUpdate={mockOnUpdate} />);

        const clearButton = screen.getByTestId('clear-all-button');
        expect(clearButton).toHaveAttribute('aria-label');
    });
});
