import { renderHook } from '@testing-library/react-hooks';

import useNestedSelectOptionChildren from '@components/components/Select/Nested/useNestedSelectOptionChildren';

const option1 = { value: '1', label: '1', isParent: true };
const option2 = { value: '5', label: '5', isParent: true };
const children1 = [{ value: '2', label: '2' }, { value: '3', label: '3' }, { value: '4', label: '4' }, option2];
const children2 = [
    { value: '6', label: '6' },
    { value: '7', label: '7' },
    { value: '8', label: '8' },
];

const parentValueToOptions = {
    [option1.value]: children1,
    [option2.value]: children2,
};

const defaultProps = {
    option: option1,
    parentValueToOptions,
    areParentsSelectable: true,
    addOptions: () => {},
};

describe('useNestedSelectOptionChildren', () => {
    it('should return props properly when parents are selectable', () => {
        const { result } = renderHook(() => useNestedSelectOptionChildren(defaultProps));

        const { children, selectableChildren, directChildren } = result.current;

        expect(children).toMatchObject([...children1, ...children2]);
        expect(selectableChildren).toMatchObject([...children1, ...children2]);
        expect(directChildren).toMatchObject(children1);
    });

    it('should return props properly when parents are not selectable', () => {
        const { result } = renderHook(() =>
            useNestedSelectOptionChildren({ ...defaultProps, areParentsSelectable: false }),
        );

        const { children, selectableChildren, directChildren } = result.current;

        expect(children).toMatchObject([...children1, ...children2]);
        expect(selectableChildren).toMatchObject([...children1, ...children2].filter((o) => o.value !== option2.value));
        expect(directChildren).toMatchObject(children1);
    });
});
