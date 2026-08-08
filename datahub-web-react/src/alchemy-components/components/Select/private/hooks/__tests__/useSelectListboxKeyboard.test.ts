import { act, renderHook } from '@testing-library/react-hooks';
import type { KeyboardEvent } from 'react';

import useSelectListboxKeyboard from '@components/components/Select/private/hooks/useSelectListboxKeyboard';
import { SelectOption } from '@components/components/Select/types';

const options: SelectOption[] = [
    { value: 'a', label: 'A' },
    { value: 'b', label: 'B' },
    { value: 'c', label: 'C' },
];

function keyEvent(key: string): KeyboardEvent<HTMLElement> {
    return {
        key,
        preventDefault: vi.fn(),
    } as unknown as KeyboardEvent<HTMLElement>;
}

describe('useSelectListboxKeyboard', () => {
    const open = vi.fn();
    const close = vi.fn();
    const toggle = vi.fn();
    const onSelectOption = vi.fn();

    beforeEach(() => {
        open.mockClear();
        close.mockClear();
        toggle.mockClear();
        onSelectOption.mockClear();
    });

    it('opens the listbox on ArrowDown when closed', () => {
        const { result } = renderHook(() =>
            useSelectListboxKeyboard({
                isOpen: false,
                options,
                selectedValues: [],
                open,
                close,
                toggle,
                onSelectOption,
            }),
        );

        act(() => {
            result.current.onTriggerKeyDown(keyEvent('ArrowDown'));
        });

        expect(open).toHaveBeenCalledTimes(1);
    });

    it('moves highlight with ArrowDown and ArrowUp when open', () => {
        const { result } = renderHook(() =>
            useSelectListboxKeyboard({
                isOpen: true,
                options,
                selectedValues: [],
                listboxId: 'test-listbox',
                open,
                close,
                toggle,
                onSelectOption,
            }),
        );

        expect(result.current.highlightedValue).toBe('a');

        act(() => {
            result.current.onTriggerKeyDown(keyEvent('ArrowDown'));
        });
        expect(result.current.highlightedValue).toBe('b');
        expect(result.current.activeDescendantId).toBe('test-listbox-option-b');

        act(() => {
            result.current.onTriggerKeyDown(keyEvent('ArrowUp'));
        });
        expect(result.current.highlightedValue).toBe('a');
    });

    it('skips disabled options when moving highlight', () => {
        const { result } = renderHook(() =>
            useSelectListboxKeyboard({
                isOpen: true,
                options,
                disabledValues: ['b'],
                selectedValues: [],
                open,
                close,
                toggle,
                onSelectOption,
            }),
        );

        expect(result.current.highlightedValue).toBe('a');

        act(() => {
            result.current.onTriggerKeyDown(keyEvent('ArrowDown'));
        });
        expect(result.current.highlightedValue).toBe('c');
    });

    it('closes on Escape when open', () => {
        const { result } = renderHook(() =>
            useSelectListboxKeyboard({
                isOpen: true,
                options,
                selectedValues: [],
                open,
                close,
                toggle,
                onSelectOption,
            }),
        );

        act(() => {
            result.current.onTriggerKeyDown(keyEvent('Escape'));
        });

        expect(close).toHaveBeenCalledTimes(1);
    });

    it('ignores keyboard when disabled', () => {
        const { result } = renderHook(() =>
            useSelectListboxKeyboard({
                isOpen: false,
                isDisabled: true,
                options,
                selectedValues: [],
                open,
                close,
                toggle,
                onSelectOption,
            }),
        );

        act(() => {
            result.current.onTriggerKeyDown(keyEvent('ArrowDown'));
            result.current.onTriggerKeyDown(keyEvent('Enter'));
            result.current.onTriggerKeyDown(keyEvent('Escape'));
        });

        expect(open).not.toHaveBeenCalled();
        expect(toggle).not.toHaveBeenCalled();
        expect(close).not.toHaveBeenCalled();
    });

    it('selects the highlighted option on Enter when open', () => {
        const { result } = renderHook(() =>
            useSelectListboxKeyboard({
                isOpen: true,
                options,
                selectedValues: [],
                open,
                close,
                toggle,
                onSelectOption,
            }),
        );

        act(() => {
            result.current.onTriggerKeyDown(keyEvent('ArrowDown'));
        });
        act(() => {
            result.current.onTriggerKeyDown(keyEvent('Enter'));
        });

        expect(onSelectOption).toHaveBeenCalledWith(options[1]);
    });
});
