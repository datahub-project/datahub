import { useMemo } from 'react';

import { NestedSelectOption } from '@components/components/Select/Nested/types';

interface Props<OptionType extends NestedSelectOption> {
    selectedOptions: OptionType[];
    option: OptionType;
    children: OptionType[];
    selectableChildren: OptionType[];
    implicitlySelectChildren: boolean;
    areParentsSelectable: boolean;
    addOptions: (nodes: OptionType[]) => void;
    removeOptions: (nodes: OptionType[]) => void;
    setSelectedOptions: React.Dispatch<React.SetStateAction<OptionType[]>>;
    handleOptionChange: (node: OptionType) => void;
    isMultiSelect: boolean;
}

export default function useNestedOption<OptionType extends NestedSelectOption>({
    selectedOptions,
    option,
    children,
    selectableChildren,
    areParentsSelectable,
    implicitlySelectChildren,
    addOptions,
    removeOptions,
    setSelectedOptions,
    handleOptionChange,
    isMultiSelect,
}: Props<OptionType>) {
    const parentChildren = useMemo(() => children.filter((c) => c.isParent), [children]);

    const areAllChildrenSelected = useMemo(
        () => selectableChildren.every((child) => selectedOptions.find((o) => o.value === child.value)),
        [selectableChildren, selectedOptions],
    );

    const areAnyChildrenSelected = useMemo(
        () => selectableChildren.some((child) => selectedOptions.find((o) => o.value === child.value)),
        [selectableChildren, selectedOptions],
    );

    const areAnyUnselectableChildrenUnexpanded = !!parentChildren.find(
        (parent) => !selectableChildren.find((child) => child.parentValue === parent.value),
    );

    const isSelected = useMemo(
        () => !!selectedOptions.find((o) => o.value === option.value),
        [selectedOptions, option.value],
    );

    const isImplicitlySelected = useMemo(
        () =>
            implicitlySelectChildren &&
            !option.isParent &&
            !!selectedOptions.find((o) => o.value === option.parentValue),
        [selectedOptions, option.isParent, option.parentValue, implicitlySelectChildren],
    );

    const isParentMissingChildren = useMemo(() => !!option.isParent && !children.length, [children, option.isParent]);

    const isPartialSelected = useMemo(
        () =>
            areParentsSelectable && !isMultiSelect
                ? false
                : (!areAllChildrenSelected && areAnyChildrenSelected) ||
                  (isSelected && isParentMissingChildren) ||
                  (isSelected && areAnyUnselectableChildrenUnexpanded) ||
                  (areAnyUnselectableChildrenUnexpanded && areAnyChildrenSelected) ||
                  (isSelected && !!children.length && !areAnyChildrenSelected) ||
                  (!isSelected &&
                      areAllChildrenSelected &&
                      !isParentMissingChildren &&
                      option.isParent &&
                      areParentsSelectable),
        [
            isSelected,
            children,
            option.isParent,
            areAllChildrenSelected,
            areAnyChildrenSelected,
            areAnyUnselectableChildrenUnexpanded,
            isParentMissingChildren,
            areParentsSelectable,
            isMultiSelect,
        ],
    );

    const selectChildrenImplicitly = () => {
        const existingSelectedOptions = new Set(selectedOptions.map((opt) => opt.value));
        const existingChildSelectedOptions = selectedOptions.filter((opt) => opt.parentValue === option.value) || [];
        if (existingSelectedOptions.has(option.value)) {
            removeOptions([option]);
        } else {
            // filter out the childrens of parent selection as we are allowing implicitly selection
            const filteredOptions = selectedOptions.filter(
                (selectedOption) => !existingChildSelectedOptions.find((o) => o.value === selectedOption.value),
            );
            const newSelectedOptions = [...filteredOptions, option];

            setSelectedOptions(newSelectedOptions);
        }
    };

    const selectOption = () => {
        if (areParentsSelectable && option.isParent && implicitlySelectChildren) {
            selectChildrenImplicitly();
        } else if (isPartialSelected || (!isSelected && !areAnyChildrenSelected)) {
            if (!isMultiSelect) {
                // Single selection behavior: replace all selections with just this option
                setSelectedOptions([option]);
            } else if (implicitlySelectChildren) {
                // Multi-selection with implicit children: add parent + children or just children
                const optionsToAdd =
                    option.isParent && !areParentsSelectable ? selectableChildren : [option, ...selectableChildren];
                addOptions(optionsToAdd);
            } else {
                // Multi-selection without implicit children: add only the clicked option
                addOptions([option]);
            }
        } else if (areAllChildrenSelected) {
            removeOptions([option, ...selectableChildren]);
        } else {
            handleOptionChange(option);
        }
    };

    return {
        selectOption,
        isPartialSelected,
        isParentMissingChildren,
        isSelected,
        isImplicitlySelected,
    };
}
