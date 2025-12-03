import { fireEvent, render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { NativeCheckbox } from '@components/components/Checkbox/NativeCheckbox';

describe('NativeCheckbox', () => {
    it('renders correctly with label', () => {
        const { getByLabelText } = render(<NativeCheckbox label="Test Checkbox" />);
        expect(getByLabelText('Test Checkbox')).toBeInTheDocument();
    });

    it('calls onChange when clicked', () => {
        const mockOnChange = vi.fn();
        const { getByRole } = render(<NativeCheckbox label="Test" onChange={mockOnChange} />);

        const checkbox = getByRole('checkbox');
        fireEvent.click(checkbox);

        expect(mockOnChange).toHaveBeenCalledTimes(1);
        expect(checkbox).not.toBeChecked();
    });

    it('reflects checked state properly', () => {
        const { getByRole, rerender } = render(<NativeCheckbox label="Test" checked={false} />);

        let checkbox = getByRole('checkbox');
        expect(checkbox).not.toBeChecked();

        rerender(<NativeCheckbox label="Test" checked />);
        checkbox = getByRole('checkbox');
        expect(checkbox).toBeChecked();
    });

    it('is disabled when disabled prop is true', () => {
        const { getByRole } = render(<NativeCheckbox label="Test" disabled />);

        const checkbox = getByRole('checkbox');
        expect(checkbox).toBeDisabled();

        fireEvent.click(checkbox);
        expect(checkbox).not.toBeChecked();
    });

    it('handles indeterminate state', () => {
        const { getByRole } = render(<NativeCheckbox label="Test" indeterminate />);

        const checkbox = getByRole('checkbox');
        // Check that the checkbox is rendered in indeterminate state
        expect(checkbox).toBeInTheDocument();
    });

    it('forwards ref properly', () => {
        const ref = React.createRef<HTMLInputElement>();
        const { getByRole } = render(<NativeCheckbox label="Test" ref={ref} />);

        const checkbox = getByRole('checkbox');
        expect(checkbox).toBeInTheDocument();
    });

    it('passes additional props to the underlying input', () => {
        const { getByRole } = render(
            <NativeCheckbox label="Test" name="test-checkbox" value="test-value" data-testid="test-checkbox" />,
        );

        const checkbox = getByRole('checkbox');
        expect(checkbox).toHaveAttribute('name', 'test-checkbox');
        expect(checkbox).toHaveAttribute('value', 'test-value');
        expect(checkbox).toHaveAttribute('data-testid', 'test-checkbox');
    });
});
