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
