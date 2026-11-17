import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import AutoCompleteSelect, { Suggestion } from '@components/components/Select/AutoCompleteSelect';

import CustomThemeProvider from '@src/CustomThemeProvider';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

type Item = { name: string };

const ALPHA: Suggestion<Item> = { value: 'a', data: { name: 'Alpha' } };
const BETA: Suggestion<Item> = { value: 'b', data: { name: 'Beta' } };

beforeEach(mockVisibilityObserver);

describe('AutoCompleteSelect', () => {
    const tree = (props: Partial<React.ComponentProps<typeof AutoCompleteSelect<Item>>>) => (
        <CustomThemeProvider>
            <AutoCompleteSelect<Item>
                render={(data) => <span>{data.name}</span>}
                emptySuggestions={[ALPHA, BETA]}
                onSearch={vi.fn()}
                placeholder="Pick one"
                {...props}
            />
        </CustomThemeProvider>
    );

    const openDropdown = () => fireEvent.click(screen.getByText('Pick one'));

    it('lists its suggestions when opened', () => {
        render(tree({}));

        openDropdown();

        expect(screen.getByText('Alpha')).toBeInTheDocument();
        expect(screen.getByText('Beta')).toBeInTheDocument();
    });

    // Callers that resolve the pre-selection through a fetch pass it after the first render.
    it('adopts an initial value that arrives late', () => {
        const { rerender } = render(tree({}));
        expect(screen.getByText('Pick one')).toBeInTheDocument();

        rerender(tree({ initialValue: ALPHA }));

        expect(screen.queryByText('Pick one')).not.toBeInTheDocument();
        expect(screen.getByText('Alpha')).toBeInTheDocument();
    });

    // The hidden input carries the selection, and reads unambiguously while a closed dropdown keeps
    // its options in the DOM.
    it('keeps what the user picked when an initial value arrives afterwards', () => {
        const onUpdate = vi.fn();
        const { rerender } = render(tree({ onUpdate, name: 'parent' }));

        openDropdown();
        fireEvent.click(screen.getByText('Beta'));
        expect(onUpdate).toHaveBeenCalledWith(BETA.data);

        rerender(tree({ onUpdate, name: 'parent', initialValue: ALPHA }));

        expect(document.querySelector('input[name="parent"]')).toHaveValue(BETA.value);
    });

    it('clears the selection when the initial value is cleared', () => {
        const { rerender } = render(tree({ initialValue: ALPHA }));
        expect(screen.getByText('Alpha')).toBeInTheDocument();

        rerender(tree({ initialValue: undefined }));

        expect(screen.getByText('Pick one')).toBeInTheDocument();
    });

    it('does not select a disabled suggestion', () => {
        const onUpdate = vi.fn();
        render(tree({ onUpdate, disabledValues: [ALPHA.value] }));

        openDropdown();
        fireEvent.click(screen.getByText('Alpha'));

        expect(onUpdate).not.toHaveBeenCalled();
        expect(screen.getByText('Pick one')).toBeInTheDocument();
    });

    // Suggestions for an earlier keystroke must not be clickable as if they matched the current one.
    it('withholds suggestions while results for the current query are loading', () => {
        const { rerender } = render(tree({ isLoading: true }));

        openDropdown();
        expect(screen.queryByText('Alpha')).not.toBeInTheDocument();

        rerender(tree({ isLoading: false }));

        expect(screen.getByText('Alpha')).toBeInTheDocument();
    });
});
