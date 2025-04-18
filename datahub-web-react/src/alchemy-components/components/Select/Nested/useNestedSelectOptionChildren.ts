import { useEffect, useMemo, useState } from 'react';

import { NestedSelectOption } from '@components/components/Select/Nested/types';

function getChildrenRecursively<OptionType extends NestedSelectOption>(
    directChildren: OptionType[],
    parentValueToOptions: { [parentValue: string]: OptionType[] },
) {
    const visitedParents = new Set<string>();
    let allChildren: OptionType[] = [];

    function getChildren(parentValue: string) {
        const newChildren = parentValueToOptions[parentValue] || [];
        if (visitedParents.has(parentValue) || !newChildren.length) {
            return;
        }

        visitedParents.add(parentValue);
        allChildren = [...allChildren, ...newChildren];
        newChildren.forEach((child) => getChildren(child.value || child.value));
    }

    directChildren.forEach((c) => getChildren(c.value || c.value));

    return allChildren;
}

interface Props<OptionType extends NestedSelectOption> {
    option: OptionType;
    parentValueToOptions: { [parentValue: string]: OptionType[] };
    areParentsSelectable: boolean;
    addOptions: (nodes: OptionType[]) => void;
}

export default function useNestedSelectOptionChildren<OptionType extends NestedSelectOption>({
    option,
    parentValueToOptions,
    areParentsSelectable,
    addOptions,
}: Props<OptionType>) {
    const [autoSelectChildren, setAutoSelectChildren] = useState(false);

    const directChildren = useMemo(
        () => parentValueToOptions[option.value] || [],
        [parentValueToOptions, option.value],
    );

    const recursiveChildren = useMemo(
        () => getChildrenRecursively(directChildren, parentValueToOptions),
        [directChildren, parentValueToOptions],
    );

    const children = useMemo(() => [...directChildren, ...recursiveChildren], [directChildren, recursiveChildren]);
    const selectableChildren = useMemo(
        () => (areParentsSelectable ? children : children.filter((c) => !c.isParent)),
        [areParentsSelectable, children],
    );
    // const parentChildren = useMemo(() => children.filter((c) => c.isParent), [children]);

    useEffect(() => {
        if (autoSelectChildren && selectableChildren.length) {
            addOptions(selectableChildren);
            setAutoSelectChildren(false);
        }
    }, [autoSelectChildren, selectableChildren, addOptions]);

    return { children, selectableChildren, directChildren, setAutoSelectChildren };
}
