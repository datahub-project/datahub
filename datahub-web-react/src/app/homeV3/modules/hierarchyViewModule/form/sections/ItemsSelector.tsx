import { Checkbox, Icon } from '@components';
import React, { useCallback, useContext, useMemo, useState } from 'react';
import styled from 'styled-components';

export type TreeNode<T = any> = T & {
    value: string;
    label: React.ReactNode | string;
    children?: TreeNode<T>[];
    hasAsyncChildren?: boolean;
};

type NodeRendererProps<T> = {
    option: TreeNode<T>;
    depth: number;
    getIsExpandable: (option: TreeNode<T>) => boolean;
    getIsExpanded: (option: TreeNode<T>) => boolean;
    toggleExpanded: (option: TreeNode<T>) => void;
    getIsSelectable: (option: TreeNode<T>) => boolean;
    getIsSelected: (option: TreeNode<T>) => boolean;
    onSelect: (option: TreeNode<T>, shouldSelectChildren: boolean) => void;
    hasSelectedChildren: (option: TreeNode<T>) => boolean;
    hasNotSelectedChildren: (option: TreeNode<T>) => boolean;
};

interface TreeViewContextType<T> {
    options: TreeNode<T>[];
    getIsExpandable: (option: TreeNode<T>) => boolean;
    getIsExpanded: (option: TreeNode<T>) => boolean;
    toggleExpanded: (option: TreeNode<T>) => void;
    getIsSelectable: (option: TreeNode<T>) => boolean;
    getIsSelected: (option: TreeNode<T>) => boolean;
    onSelect: (option: TreeNode<T>, shouldSelectChildren: boolean) => void;
    hasSelectedChildren: (option: TreeNode<T>) => boolean;
    hasNotSelectedChildren: (option: TreeNode<T>) => boolean;
    customOptionRenderer?: (props: NodeRendererProps<T>) => React.ReactNode;
    loadAsyncChildren?: (option: TreeNode<T>) => void;
}

const DefaultTreeViewContext: TreeViewContextType<any> = {
    options: [],
    getIsExpandable: () => false,
    getIsExpanded: () => false,
    toggleExpanded: () => null,
    getIsSelectable: () => false,
    getIsSelected: () => false,
    hasSelectedChildren: () => false,
    hasNotSelectedChildren: () => false,
    onSelect: () => null,
};

const TreeViewContext = React.createContext<TreeViewContextType<any>>(DefaultTreeViewContext);

interface TreeViewContextProviderProps<T> {
    options: TreeNode<T>[];
    customOptionRenderer?: (props: NodeRendererProps<T>) => React.ReactNode;
    loadAsyncChildren?: (option: TreeNode<T>) => void;
}

function TreeViewContextProvider<T>({
    children,
    options,
    customOptionRenderer,
    loadAsyncChildren,
}: React.PropsWithChildren<TreeViewContextProviderProps<T>>) {
    const [selectedValues, setSelectedValues] = useState<string[]>([]);
    const [expandedValues, setExpandedValues] = useState<string[]>([]);

    // for async loaded children options
    const [loadedValues, setLoadedValues] = useState<string[]>([]);

    const getIsExpandable = useCallback((option: TreeNode<T>) => {
        return !!option?.children?.length || option.hasAsyncChildren;
    }, []);

    const getIsExpanded = useCallback(
        (option: TreeNode<T>) => {
            return expandedValues.includes(option.value);
        },
        [expandedValues],
    );

    const toggleExpanded = useCallback(
        (option: TreeNode<T>) => {
            const isExpanded = getIsExpanded(option);

            if (!isExpanded) {
                // Expand
                setExpandedValues((prev) => [...prev, option.value]);
                if (option.hasAsyncChildren && !loadedValues.includes(option.value)) {
                    console.log('>>> run async loading', option, loadAsyncChildren)
                    loadAsyncChildren?.(option);
                    // TODO: uncomment
                    // setLoadedValues(prev => [...prev, option.value])
                }
            } else {
                // Collapse
                const values = getAllValues([option]);
                setExpandedValues((prev) => prev.filter((value) => !values.includes(value)));
            }
        },
        [getIsExpanded, loadAsyncChildren, loadedValues],
    );

    const getIsSelectable = useCallback((_option: TreeNode<T>) => {
        // TODO: implement
        return true;
    }, []);

    const getIsSelected = useCallback(
        (option: TreeNode<T>) => {
            return selectedValues.includes(option.value);
        },
        [selectedValues],
    );

    const hasSelectedChildren = useCallback(
        (option: TreeNode<T>) => {
            const childrenValues = getAllValues(option.children ?? []);
            return childrenValues.some((childrenValue) => selectedValues.includes(childrenValue));
        },
        [selectedValues],
    );

    const hasNotSelectedChildren = useCallback(
        (option: TreeNode<T>) => {
            const childrenValues = getAllValues(option.children ?? []);
            return childrenValues.some((childrenValue) => !selectedValues.includes(childrenValue));
        },
        [selectedValues],
    );

    const onSelect = useCallback(
        (option: TreeNode<T>, shouldSelectChildren: boolean) => {
            const isAlreadySelected = getIsSelected(option);

            const valuesToToggleSelect = shouldSelectChildren ? getAllValues([option]) : [option.value];

            if (isAlreadySelected) {
                // Deselect option and its children
                setSelectedValues((prev) => prev.filter((item) => !valuesToToggleSelect.includes(item)));
            } else {
                setSelectedValues((prev) => [...prev, ...valuesToToggleSelect]);
            }
        },
        [getIsSelected],
    );

    return (
        <TreeViewContext.Provider
            value={{
                options,
                getIsSelected,
                onSelect,
                getIsSelectable,
                getIsExpandable,
                toggleExpanded,
                getIsExpanded,
                hasSelectedChildren,
                hasNotSelectedChildren,
                customOptionRenderer,
                loadAsyncChildren,
            }}
        >
            {children}
        </TreeViewContext.Provider>
    );
}

interface ItemsSelectorProps<T> {
    options: TreeNode<T>[];
    values?: string[];
    // shouldSelectChildren?: boolean;
    loadAsyncChildren?: (option: TreeNode<T>) => void;
}

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

interface NodeProps<T> {
    option: TreeNode<T>;
    depth: number;
}

const NodeWrapper = styled.div<{ $depth?: number; $hasChildren?: boolean }>`
    display: flex;
    padding-left: calc(8px * ${(props) => props.$depth} + ${(props) => (!props.$hasChildren ? '12px' : '0px')});
`;

function DefaultNodeRenderer<T>({
    option,
    depth,
    getIsExpanded,
    getIsExpandable,
    toggleExpanded,
    onSelect,
    getIsSelectable,
    getIsSelected,
    hasSelectedChildren,
    hasNotSelectedChildren,
}: NodeRendererProps<T>) {
    const isExpanded = getIsExpanded(option);
    const isExpandable = getIsExpandable(option);
    const isSelected = getIsSelected(option);
    const isSelectable = getIsSelectable(option);

    // const hasChildren = !!option.children?.length;

    // TODO: implement
    // has selected children but not all of them
    const isIntermediate = hasSelectedChildren(option) && hasNotSelectedChildren(option);

    return (
        <NodeWrapper $depth={depth} $hasChildren={isExpandable}>
            <ExpandToggler isExpanded={isExpanded} expandable={isExpandable} toggle={() => toggleExpanded(option)} />

            {option.label}

            <SpaceFiller />

            {isSelectable && (
                <Checkbox
                    isChecked={isSelected}
                    setIsChecked={() => onSelect(option, true)}
                    isIntermediate={isIntermediate}
                />
            )}
        </NodeWrapper>
    );
}

// interface NodeRendererProps<T> extends NodeProps<T> {
//     render?: React.FC<NodeProps<T>>;
//     onExpandCollapseToggle: (option: TreeNode<T>) => void;
// }

interface ExpandTogglerProps {
    isExpanded: boolean;
    expandable: boolean;
    toggle: () => void;
}

function ExpandToggler({ isExpanded, expandable, toggle }: ExpandTogglerProps) {
    if (!expandable) return null;
    return <Icon icon="CaretRight" source="phosphor" rotate={isExpanded ? '90' : '0'} size="lg" onClick={toggle} />;
}

function Node<T>({ option, depth }: NodeProps<T>) {
    const {
        customOptionRenderer,
        getIsExpandable,
        getIsExpanded,
        toggleExpanded,
        getIsSelectable,
        getIsSelected,
        onSelect,
        hasSelectedChildren,
        hasNotSelectedChildren,
    } = useContext<TreeViewContextType<T>>(TreeViewContext);

    const isExpanded = getIsExpanded(option);

    const rendererProps: NodeRendererProps<T> = useMemo(
        () => ({
            option,
            depth: depth + 1,
            getIsExpandable,
            getIsExpanded,
            toggleExpanded,
            getIsSelectable,
            getIsSelected,
            onSelect,
            hasSelectedChildren,
            hasNotSelectedChildren,
        }),
        [
            option,
            depth,
            getIsExpandable,
            getIsExpanded,
            toggleExpanded,
            getIsSelectable,
            getIsSelected,
            onSelect,
            hasSelectedChildren,
            hasNotSelectedChildren,
        ],
    );

    const render = useCallback(
        (props: NodeRendererProps<T>) => {
            if (customOptionRenderer) return customOptionRenderer(props);

            return <DefaultNodeRenderer {...props} />;
        },
        [customOptionRenderer],
    );

    return (
        <>
            {render(rendererProps)}

            {isExpanded
                ? (option.children ?? []).map((nestedOption) => (
                      <Node<T> option={nestedOption} depth={depth + 1} key={nestedOption.value} />
                  ))
                : null}
        </>
    );
}

function getAllValues<T>(options: TreeNode<T>[]) {
    const values: string[] = [];

    function traverse(item: TreeNode<T>) {
        values.push(item.value);
        if (item.children && item.children.length > 0) {
            item.children.forEach(traverse);
        }
    }

    options.forEach(traverse);
    return values;
}

export default function ItemsSelector<T>({ options, loadAsyncChildren }: ItemsSelectorProps<T>) {
    return (
        <TreeViewContextProvider options={options} loadAsyncChildren={loadAsyncChildren}>
            {options.map((option) => (
                <Node<T> option={option} depth={0} key={option.value} />
            ))}
        </TreeViewContextProvider>
    );
}
