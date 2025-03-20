import { useMemo } from 'react';
import { SelectOption } from './types';

interface Props {
    selectedOptions: SelectOption[];
    option: SelectOption;
    children: SelectOption[];
    selectableChildren: SelectOption[];
    implicitlySelectChildren: boolean;
    areParentsSelectable: boolean;
    addOptions: (nodes: SelectOption[]) => void;
    removeOptions: (nodes: SelectOption[]) => void;
    setSelectedOptions: React.Dispatch<React.SetStateAction<SelectOption[]>>;
    handleOptionChange: (node: SelectOption) => void;
}

export default function useNestedOption({
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
}: Props) {
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
        () =>
            !!selectedOptions.find((o) => o.value === option.value) ||
            (!areParentsSelectable &&
                !!option.isParent &&
                !!selectableChildren.length &&
                areAllChildrenSelected &&
                !areAnyUnselectableChildrenUnexpanded),
        [
            selectedOptions,
            areAllChildrenSelected,
            areAnyUnselectableChildrenUnexpanded,
            areParentsSelectable,
            option.isParent,
            option.value,
            selectableChildren.length,
        ],
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
            (!areAllChildrenSelected && areAnyChildrenSelected) ||
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
            const optionsToAdd =
                option.isParent && !areParentsSelectable ? selectableChildren : [option, ...selectableChildren];

            addOptions(optionsToAdd);
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
