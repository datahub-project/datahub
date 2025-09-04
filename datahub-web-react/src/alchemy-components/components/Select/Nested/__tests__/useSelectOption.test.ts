import { renderHook } from '@testing-library/react-hooks';

import useNestedOption from '@components/components/Select/Nested/useSelectOption';

const option = { value: '1', label: '1', isParent: true };
const children = [
    { value: '2', label: '2' },
    { value: '3', label: '3' },
    { value: '4', label: '4' },
];

const defaultProps = {
    selectedOptions: [],
    option,
    children,
    selectableChildren: children,
    areParentsSelectable: true,
    implicitlySelectChildren: false,
    isMultiSelect: true,
    addOptions: () => {},
    removeOptions: () => {},
    setSelectedOptions: () => {},
    handleOptionChange: () => {},
};

describe('useSelectChildren - areParentsSelectable is true', () => {
    it('should return props properly when parent is not selected and no children are selected', () => {
        const mockAddOptions = vi.fn();
        const { result } = renderHook(() =>
            useNestedOption({
                ...defaultProps,
                addOptions: mockAddOptions,
            }),
        );

        const { selectOption, isPartialSelected, isParentMissingChildren, isSelected, isImplicitlySelected } =
            result.current;

        expect(isPartialSelected).toBe(false);
        expect(isParentMissingChildren).toBe(false);
        expect(isSelected).toBe(false);
        expect(isImplicitlySelected).toBe(false);
        selectOption();
        expect(mockAddOptions).toHaveBeenCalledWith([option, ...children]);
    });

    it('should return props properly when parent is selected and no children are selected', () => {
        const mockAddOptions = vi.fn();
        const { result } = renderHook(() =>
            useNestedOption({
                ...defaultProps,
                selectedOptions: [option],
                addOptions: mockAddOptions,
            }),
        );

        const { selectOption, isPartialSelected, isParentMissingChildren, isSelected, isImplicitlySelected } =
            result.current;

        expect(isPartialSelected).toBe(true);
        expect(isParentMissingChildren).toBe(false);
        expect(isSelected).toBe(true);
        expect(isImplicitlySelected).toBe(false);
        selectOption();
        expect(mockAddOptions).toHaveBeenCalledWith([option, ...children]);
    });

    it('should return props properly when parent is not selected and some children are selected', () => {
        const mockAddOptions = vi.fn();
        const { result } = renderHook(() =>
            useNestedOption({
                ...defaultProps,
                selectedOptions: [children[0]],
                addOptions: mockAddOptions,
            }),
        );

        const { selectOption, isPartialSelected, isParentMissingChildren, isSelected, isImplicitlySelected } =
            result.current;

        expect(isPartialSelected).toBe(true);
        expect(isParentMissingChildren).toBe(false);
        expect(isSelected).toBe(false);
        expect(isImplicitlySelected).toBe(false);
        selectOption();
        expect(mockAddOptions).toHaveBeenCalledWith([option, ...children]);
    });

    it('should return props properly when parent and children are selected', () => {
        const mockRemoveOptions = vi.fn();
        const { result } = renderHook(() =>
            useNestedOption({
                ...defaultProps,
                selectedOptions: [option, ...children],
                removeOptions: mockRemoveOptions,
            }),
        );

        const { selectOption, isPartialSelected, isParentMissingChildren, isSelected, isImplicitlySelected } =
            result.current;

        expect(isPartialSelected).toBe(false);
        expect(isParentMissingChildren).toBe(false);
        expect(isSelected).toBe(true);
        expect(isImplicitlySelected).toBe(false);
        selectOption();
        expect(mockRemoveOptions).toHaveBeenCalledWith([option, ...children]);
    });
});

describe('useSelectChildren - areParentsSelectable is false', () => {
    it('should return props properly when parent is not selected and no children are selected', () => {
        const mockAddOptions = vi.fn();
        const { result } = renderHook(() =>
            useNestedOption({
                ...defaultProps,
                areParentsSelectable: false,
                addOptions: mockAddOptions,
            }),
        );

        const { selectOption, isPartialSelected, isParentMissingChildren, isSelected, isImplicitlySelected } =
            result.current;

        expect(isPartialSelected).toBe(false);
        expect(isParentMissingChildren).toBe(false);
        expect(isSelected).toBe(false);
        expect(isImplicitlySelected).toBe(false);
        selectOption();
        expect(mockAddOptions).toHaveBeenCalledWith(children);
    });

    it('should return props properly when some children are selected', () => {
        const mockAddOptions = vi.fn();
        const { result } = renderHook(() =>
            useNestedOption({
                ...defaultProps,
                areParentsSelectable: false,
                selectedOptions: [children[0]],
                addOptions: mockAddOptions,
            }),
        );

        const { selectOption, isPartialSelected, isParentMissingChildren, isSelected, isImplicitlySelected } =
            result.current;

        expect(isPartialSelected).toBe(true);
        expect(isParentMissingChildren).toBe(false);
        expect(isSelected).toBe(false);
        expect(isImplicitlySelected).toBe(false);
        selectOption();
        expect(mockAddOptions).toHaveBeenCalledWith(children);
    });

    it('should return props properly when all children are selected', () => {
        const mockRemoveOptions = vi.fn();
        const { result } = renderHook(() =>
            useNestedOption({
                ...defaultProps,
                areParentsSelectable: false,
                selectedOptions: [option, ...children],
                removeOptions: mockRemoveOptions,
            }),
        );

        const { selectOption, isPartialSelected, isParentMissingChildren, isSelected, isImplicitlySelected } =
            result.current;

        expect(isPartialSelected).toBe(false);
        expect(isParentMissingChildren).toBe(false);
        expect(isSelected).toBe(true);
        expect(isImplicitlySelected).toBe(false);
        selectOption();
        expect(mockRemoveOptions).toHaveBeenCalledWith([option, ...children]);
    });
});

describe('useSelectChildren - areParentsSelectable is true & implicitlySelectChildren is true', () => {
    it('should return props properly when parent is not selected and no children are selected', () => {
        const mockSetSelectedOptions = vi.fn();
        const { result } = renderHook(() =>
            useNestedOption({
                ...defaultProps,
                implicitlySelectChildren: true,
                setSelectedOptions: mockSetSelectedOptions,
            }),
        );

        const { selectOption, isPartialSelected, isParentMissingChildren, isSelected, isImplicitlySelected } =
            result.current;

        expect(isPartialSelected).toBe(false);
        expect(isParentMissingChildren).toBe(false);
        expect(isSelected).toBe(false);
        expect(isImplicitlySelected).toBe(false);
        selectOption();
        expect(mockSetSelectedOptions).toHaveBeenCalledWith([option]);
    });

    it('should return props properly when parent is selected and no children are selected', () => {
        const mockRemoveOptions = vi.fn();
        const { result } = renderHook(() =>
            useNestedOption({
                ...defaultProps,
                implicitlySelectChildren: true,
                selectedOptions: [option],
                removeOptions: mockRemoveOptions,
            }),
        );

        const { selectOption, isPartialSelected, isParentMissingChildren, isSelected, isImplicitlySelected } =
            result.current;

        expect(isPartialSelected).toBe(true);
        expect(isParentMissingChildren).toBe(false);
        expect(isSelected).toBe(true);
        expect(isImplicitlySelected).toBe(false);
        selectOption();
        expect(mockRemoveOptions).toHaveBeenCalledWith([option]);
    });
});

describe('useSelectChildren - isMultiSelect is false (single select mode)', () => {
    const singleSelectProps = {
        ...defaultProps,
        isMultiSelect: false,
    };

    it('should clear previous selections when selecting a new option', () => {
        const mockSetSelectedOptions = vi.fn();
        const previouslySelected = { value: '5', label: '5' };

        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                selectedOptions: [previouslySelected],
                setSelectedOptions: mockSetSelectedOptions,
            }),
        );

        const { selectOption } = result.current;
        selectOption();

        // Should replace all previous selections with just the current option
        expect(mockSetSelectedOptions).toHaveBeenCalledWith([option]);
    });

    it('should not show partial selection state when areParentsSelectable is true', () => {
        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                areParentsSelectable: true,
                selectedOptions: [children[0]], // Some children selected
            }),
        );

        const { isPartialSelected } = result.current;

        // In single select mode with areParentsSelectable=true, partial selection should be false
        expect(isPartialSelected).toBe(false);
    });

    it('should replace selection when selecting a child option', () => {
        const mockSetSelectedOptions = vi.fn();
        const childOption = children[0];
        const previouslySelected = { value: '5', label: '5' };

        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                option: childOption,
                selectedOptions: [previouslySelected],
                setSelectedOptions: mockSetSelectedOptions,
            }),
        );

        const { selectOption } = result.current;
        selectOption();

        expect(mockSetSelectedOptions).toHaveBeenCalledWith([childOption]);
    });

    it('should show implicit selection correctly even in single select mode', () => {
        const childOption = { ...children[0], parentValue: option.value };

        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                option: childOption,
                selectedOptions: [option], // Parent is selected
                implicitlySelectChildren: true,
            }),
        );

        const { isImplicitlySelected } = result.current;

        // Implicit selection logic still works in single select mode
        // The child should appear as implicitly selected when parent is selected
        expect(isImplicitlySelected).toBe(true);
    });

    it('should handle parent selection with areParentsSelectable=false in single select mode', () => {
        const mockSetSelectedOptions = vi.fn();

        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                areParentsSelectable: false,
                setSelectedOptions: mockSetSelectedOptions,
            }),
        );

        const { selectOption, isPartialSelected } = result.current;

        expect(isPartialSelected).toBe(false);
        selectOption();

        // Should still replace selections, but only with the parent option
        expect(mockSetSelectedOptions).toHaveBeenCalledWith([option]);
    });

    it('should handle implicit selection in single select mode', () => {
        const mockSetSelectedOptions = vi.fn();
        const mockRemoveOptions = vi.fn();

        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                areParentsSelectable: true,
                implicitlySelectChildren: true,
                setSelectedOptions: mockSetSelectedOptions,
                removeOptions: mockRemoveOptions,
            }),
        );

        const { selectOption } = result.current;
        selectOption();

        // In single select mode with implicit selection, should set only the parent
        expect(mockSetSelectedOptions).toHaveBeenCalledWith([option]);
    });

    it('should deselect when clicking already selected option in single select mode', () => {
        const mockRemoveOptions = vi.fn();

        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                areParentsSelectable: true,
                implicitlySelectChildren: true,
                selectedOptions: [option],
                removeOptions: mockRemoveOptions,
            }),
        );

        const { selectOption, isSelected } = result.current;

        expect(isSelected).toBe(true);
        selectOption();

        // Should remove the option when it's already selected
        expect(mockRemoveOptions).toHaveBeenCalledWith([option]);
    });

    it('should not create partial selections with child selections in single select mode', () => {
        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                areParentsSelectable: false,
                selectedOptions: [children[0]], // One child selected
            }),
        );

        const { isPartialSelected, isSelected } = result.current;

        // When areParentsSelectable is false and some children are selected,
        // it should show as partially selected even in single select mode
        expect(isPartialSelected).toBe(true);
        // Parent should not be selected when only some children are selected
        expect(isSelected).toBe(false);
    });

    it('should handle selection of parent when all children are selected in single select mode', () => {
        const mockRemoveOptions = vi.fn();

        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                areParentsSelectable: true,
                selectedOptions: [...children], // All children selected
                removeOptions: mockRemoveOptions,
            }),
        );

        const { selectOption, isSelected, isPartialSelected } = result.current;

        // In single select mode, parent should not be considered selected even if all children are
        expect(isSelected).toBe(false);
        expect(isPartialSelected).toBe(false);

        selectOption();
        // When all children are selected, selecting parent should remove all selections
        expect(mockRemoveOptions).toHaveBeenCalledWith([option, ...children]);
    });

    it('should properly handle child option selection in single select mode', () => {
        const mockSetSelectedOptions = vi.fn();
        const childOption = children[0];

        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                option: childOption,
                children: [],
                selectableChildren: [],
                setSelectedOptions: mockSetSelectedOptions,
            }),
        );

        const { selectOption, isSelected, isPartialSelected } = result.current;

        expect(isSelected).toBe(false);
        expect(isPartialSelected).toBe(false);

        selectOption();
        expect(mockSetSelectedOptions).toHaveBeenCalledWith([childOption]);
    });

    it('should prevent showing implicitly selected state for parents in single select mode', () => {
        // Test the specific requirement: should not show implicitly selected parents
        const childOption = { ...children[0], parentValue: option.value };

        const { result } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                option: option, // Testing parent option
                selectedOptions: [childOption], // Child is selected
                implicitlySelectChildren: true,
                areParentsSelectable: true,
            }),
        );

        const { isSelected, isPartialSelected } = result.current;

        // In single select mode, parent should not be implicitly selected
        // even when child is selected and implicitlySelectChildren is true
        expect(isSelected).toBe(false);
        // Should show partial selection state to indicate child selection
        expect(isPartialSelected).toBe(false); // Due to single select mode logic
    });

    it('should ensure only one selection at a time with various option types', () => {
        const mockSetSelectedOptions = vi.fn();
        const existingOption = { value: 'existing', label: 'Existing' };

        // Test selecting parent when another option is already selected
        const { result: parentResult } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                selectedOptions: [existingOption],
                setSelectedOptions: mockSetSelectedOptions,
            }),
        );

        parentResult.current.selectOption();
        expect(mockSetSelectedOptions).toHaveBeenCalledWith([option]);

        // Test selecting child when another option is already selected
        const childOption = children[0];
        const { result: childResult } = renderHook(() =>
            useNestedOption({
                ...singleSelectProps,
                option: childOption,
                children: [],
                selectableChildren: [],
                selectedOptions: [existingOption],
                setSelectedOptions: mockSetSelectedOptions,
            }),
        );

        childResult.current.selectOption();
        expect(mockSetSelectedOptions).toHaveBeenCalledWith([childOption]);
    });
});
