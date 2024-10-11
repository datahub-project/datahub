import React, { useState, useMemo, useEffect } from 'react';

import { colors, Icon } from '@components';
import theme from '@components/theme';
import styled from 'styled-components';
import { Checkbox } from 'antd';

import { OptionLabel } from '../components';
import { SelectOption } from './types';

const ParentOption = styled.div`
    display: flex;
    align-items: center;
`;

const ChildOptions = styled.div`
    padding-left: 20px;
`;

const StyledCheckbox = styled(Checkbox)<{ checked: boolean; indeterminate: boolean }>`
    .ant-checkbox-inner {
        border: 1px solid ${colors.gray[300]} !important;
        border-radius: 3px;
    }
    margin-left: auto;
    ${(props) =>
        props.checked &&
        !props.indeterminate &&
        `
		.ant-checkbox-inner {
			background-color: ${theme.semanticTokens.colors.primary};
			border-color: ${theme.semanticTokens.colors.primary} !important;
		}
	`}
    ${(props) =>
        props.indeterminate &&
        `
		.ant-checkbox-inner {
			&:after {
				background-color: ${theme.semanticTokens.colors.primary};
			}
		}
	`}
    ${(props) =>
        props.disabled &&
        `
		.ant-checkbox-inner {
			background-color: ${colors.gray[200]} !important;
		}
	`}
`;

function getChildrenRecursively(
    directChildren: SelectOption[],
    parentValueToOptions: { [parentValue: string]: SelectOption[] },
) {
    const visitedParents = new Set<string>();
    let allChildren: SelectOption[] = [];

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

interface OptionProps {
    option: SelectOption;
    selectedOptions: SelectOption[];
    parentValueToOptions: { [parentValue: string]: SelectOption[] };
    areParentsSelectable: boolean;
    handleOptionChange: (node: SelectOption) => void;
    addOptions: (nodes: SelectOption[]) => void;
    removeOptions: (nodes: SelectOption[]) => void;
    loadData?: (node: SelectOption) => void;
    isMultiSelect?: boolean;
    alwaysReloadParentData?: boolean;
}

export const NestedOption = ({
    option,
    selectedOptions,
    parentValueToOptions,
    handleOptionChange,
    addOptions,
    removeOptions,
    loadData,
    alwaysReloadParentData,
    isMultiSelect,
    areParentsSelectable,
}: OptionProps) => {
    const [autoSelectChildren, setAutoSelectChildren] = useState(false);
    const [isOpen, setIsOpen] = useState(false);
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
    const parentChildren = useMemo(() => children.filter((c) => c.isParent), [children]);

    useEffect(() => {
        if (autoSelectChildren && selectableChildren.length) {
            addOptions(selectableChildren);
            setAutoSelectChildren(false);
        }
    }, [autoSelectChildren, selectableChildren, addOptions]);

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

    const isParentMissingChildren = useMemo(() => !!option.isParent && !children.length, [children, option.isParent]);

    const isPartialSelected = useMemo(
        () =>
            (!areAllChildrenSelected && areAnyChildrenSelected) ||
            (isSelected && isParentMissingChildren) ||
            (isSelected && areAnyUnselectableChildrenUnexpanded) ||
            (areAnyUnselectableChildrenUnexpanded && areAnyChildrenSelected) ||
            (isSelected && !!children.length && !areAnyChildrenSelected),
        [
            isSelected,
            children,
            areAllChildrenSelected,
            areAnyChildrenSelected,
            areAnyUnselectableChildrenUnexpanded,
            isParentMissingChildren,
        ],
    );

    const selectOption = () => {
        if (isPartialSelected || (!isSelected && !areAnyChildrenSelected)) {
            const optionsToAdd =
                option.isParent && !areParentsSelectable ? selectableChildren : [option, ...selectableChildren];
            addOptions(optionsToAdd);
        } else if (areAllChildrenSelected) {
            removeOptions([option, ...selectableChildren]);
        } else {
            handleOptionChange(option);
        }
    };

    return (
        <div>
            <ParentOption>
                <OptionLabel
                    key={option.value}
                    onClick={(e) => {
                        if (isParentMissingChildren || alwaysReloadParentData) {
                            loadData?.(option);
                        }
                        if (option.isParent) {
                            setIsOpen(!isOpen);
                        } else {
                            selectOption();
                        }
                        e.preventDefault();
                    }}
                    isSelected={!isMultiSelect && isSelected}
                    style={{ width: '100%' }}
                >
                    {option.isParent && <strong>{option.label}</strong>}
                    {!option.isParent && <>{option.label}</>}
                    {option.isParent && (
                        <Icon
                            onClick={(e) => {
                                e.stopPropagation();
                                e.preventDefault();
                                setIsOpen(!isOpen);
                                if (!isOpen && (isParentMissingChildren || alwaysReloadParentData)) {
                                    loadData?.(option);
                                }
                            }}
                            icon="ChevronLeft"
                            rotate={isOpen ? '90' : '270'}
                            size="xl"
                            color="gray"
                            style={{ cursor: 'pointer', marginLeft: '4px' }}
                        />
                    )}
                    <StyledCheckbox
                        checked={isSelected}
                        indeterminate={isPartialSelected}
                        onClick={(e) => {
                            e.stopPropagation();
                            e.preventDefault();
                            if (isParentMissingChildren || alwaysReloadParentData) {
                                loadData?.(option);
                                setAutoSelectChildren(true);
                            }
                            selectOption();
                        }}
                    />
                </OptionLabel>
            </ParentOption>
            {isOpen && (
                <ChildOptions>
                    {directChildren.map((child) => (
                        <NestedOption
                            key={child.value}
                            selectedOptions={selectedOptions}
                            option={child}
                            parentValueToOptions={parentValueToOptions}
                            addOptions={addOptions}
                            handleOptionChange={handleOptionChange}
                            loadData={loadData}
                            removeOptions={removeOptions}
                            isMultiSelect={isMultiSelect}
                            areParentsSelectable={areParentsSelectable}
                        />
                    ))}
                </ChildOptions>
            )}
        </div>
    );
};
