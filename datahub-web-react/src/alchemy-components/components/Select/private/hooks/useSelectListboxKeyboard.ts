import { useCallback, useEffect, useMemo, useState, type KeyboardEvent } from 'react';

import { SelectOption } from '@components/components/Select/types';

const EMPTY_DISABLED_VALUES: string[] = [];

interface UseSelectListboxKeyboardArgs {
    isOpen: boolean;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isMultiSelect?: boolean;
    optionSwitchable?: boolean;
    options: SelectOption[];
    disabledValues?: string[];
    selectedValues: string[];
    listboxId?: string;
    open: () => void;
    close: () => void;
    toggle: () => void;
    onSelectOption: (option: SelectOption) => void;
    onClearSelection?: () => void;
}

interface UseSelectListboxKeyboardResult {
    highlightedValue: string | undefined;
    activeDescendantId: string | undefined;
    getOptionId: (value: string) => string | undefined;
    isOptionHighlighted: (value: string) => boolean;
    setHighlightedValue: (value: string | undefined) => void;
    onTriggerKeyDown: (event: KeyboardEvent<HTMLElement>) => void;
}

function getEnabledIndexes(options: SelectOption[], disabledValues: string[]): number[] {
    return options.reduce<number[]>((indexes, option, index) => {
        if (!disabledValues.includes(option.value)) indexes.push(index);
        return indexes;
    }, []);
}

/**
 * Keyboard model for alchemy Select listboxes.
 *
 * Keeps focus on the trigger and moves an active option via aria-activedescendant
 * so ArrowUp/ArrowDown do not scroll surrounding drawers/pages.
 */
export default function useSelectListboxKeyboard({
    isOpen,
    isDisabled,
    isReadOnly,
    isMultiSelect,
    optionSwitchable,
    options,
    disabledValues = EMPTY_DISABLED_VALUES,
    selectedValues,
    listboxId,
    open,
    close,
    toggle,
    onSelectOption,
    onClearSelection,
}: UseSelectListboxKeyboardArgs): UseSelectListboxKeyboardResult {
    const [highlightedIndex, setHighlightedIndex] = useState(-1);

    const enabledIndexes = useMemo(() => getEnabledIndexes(options, disabledValues), [options, disabledValues]);

    useEffect(() => {
        if (!isOpen) {
            setHighlightedIndex(-1);
            return;
        }

        // Preserve the current highlight across re-renders. Only seed when none is set
        // or the current index is no longer valid (e.g. filtered options changed).
        setHighlightedIndex((current) => {
            const currentOption = current >= 0 ? options[current] : undefined;
            if (currentOption && !disabledValues.includes(currentOption.value)) {
                return current;
            }

            const selectedEnabledIndex = options.findIndex(
                (option) => selectedValues.includes(option.value) && !disabledValues.includes(option.value),
            );
            if (selectedEnabledIndex >= 0) return selectedEnabledIndex;
            return enabledIndexes[0] ?? -1;
        });
    }, [isOpen, options, selectedValues, disabledValues, enabledIndexes]);

    const getOptionId = useCallback(
        (value: string) => (listboxId ? `${listboxId}-option-${value}` : undefined),
        [listboxId],
    );

    const highlightedValue = highlightedIndex >= 0 ? options[highlightedIndex]?.value : undefined;
    const activeDescendantId = highlightedValue ? getOptionId(highlightedValue) : undefined;

    const moveHighlight = useCallback(
        (direction: 1 | -1) => {
            if (!enabledIndexes.length) return;
            setHighlightedIndex((current) => {
                const currentEnabledPos = enabledIndexes.indexOf(current);
                if (currentEnabledPos === -1) {
                    return direction === 1 ? enabledIndexes[0] : enabledIndexes[enabledIndexes.length - 1];
                }
                const nextPos = (currentEnabledPos + direction + enabledIndexes.length) % enabledIndexes.length;
                return enabledIndexes[nextPos];
            });
        },
        [enabledIndexes],
    );

    const activateHighlighted = useCallback(() => {
        const option = highlightedIndex >= 0 ? options[highlightedIndex] : undefined;
        if (!option || disabledValues.includes(option.value)) return;

        if (!isMultiSelect && optionSwitchable && selectedValues.includes(option.value)) {
            onClearSelection?.();
            return;
        }
        onSelectOption(option);
    }, [
        disabledValues,
        highlightedIndex,
        isMultiSelect,
        onClearSelection,
        onSelectOption,
        optionSwitchable,
        options,
        selectedValues,
    ]);

    const onTriggerKeyDown = useCallback(
        (event: KeyboardEvent<HTMLElement>) => {
            if (isDisabled || isReadOnly) return;

            switch (event.key) {
                case 'ArrowDown': {
                    event.preventDefault();
                    if (!isOpen) {
                        open();
                        return;
                    }
                    moveHighlight(1);
                    break;
                }
                case 'ArrowUp': {
                    event.preventDefault();
                    if (!isOpen) {
                        open();
                        return;
                    }
                    moveHighlight(-1);
                    break;
                }
                case 'Home': {
                    if (!isOpen || !enabledIndexes.length) return;
                    event.preventDefault();
                    setHighlightedIndex(enabledIndexes[0]);
                    break;
                }
                case 'End': {
                    if (!isOpen || !enabledIndexes.length) return;
                    event.preventDefault();
                    setHighlightedIndex(enabledIndexes[enabledIndexes.length - 1]);
                    break;
                }
                case 'Enter':
                case ' ': {
                    event.preventDefault();
                    if (!isOpen) {
                        toggle();
                        return;
                    }
                    activateHighlighted();
                    break;
                }
                case 'Escape': {
                    if (!isOpen) return;
                    event.preventDefault();
                    close();
                    break;
                }
                default:
                    break;
            }
        },
        [
            activateHighlighted,
            close,
            enabledIndexes,
            isDisabled,
            isOpen,
            isReadOnly,
            moveHighlight,
            open,
            toggle,
        ],
    );

    const setHighlightedValue = useCallback(
        (value: string | undefined) => {
            if (!value) {
                setHighlightedIndex(-1);
                return;
            }
            const index = options.findIndex((option) => option.value === value);
            setHighlightedIndex(index);
        },
        [options],
    );

    const isOptionHighlighted = useCallback(
        (value: string) => isOpen && highlightedValue === value,
        [highlightedValue, isOpen],
    );

    return {
        highlightedValue,
        activeDescendantId,
        getOptionId,
        isOptionHighlighted,
        setHighlightedValue,
        onTriggerKeyDown,
    };
}
