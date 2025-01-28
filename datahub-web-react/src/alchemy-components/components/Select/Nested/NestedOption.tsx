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

const StyledCheckbox = styled(Checkbox)<{ checked: boolean; indeterminate?: boolean }>`
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
    isLoadingParentChildList?: boolean;
    setSelectedOptions: React.Dispatch<React.SetStateAction<SelectOption[]>>;
    hideParentCheckbox?: boolean;
}

export const NestedOption = ({
    option,
    selectedOptions,
    parentValueToOptions,
    handleOptionChange,
    addOptions,
    removeOptions,
    loadData,
    isMultiSelect,
    areParentsSelectable,
    isLoadingParentChildList,
    setSelectedOptions,
    hideParentCheckbox,
}: OptionProps) => {
    const [autoSelectChildren, setAutoSelectChildren] = useState(false);
    const [loadingParentUrns, setLoadingParentUrns] = useState<string[]>([]);
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

    const isImplicitlySelected = useMemo(
        () => !option.isParent && !!selectedOptions.find((o) => o.value === option.parentValue),
        [selectedOptions, option.isParent, option.parentValue],
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
        if (areParentsSelectable && option.isParent) {
            const existingSelectedOptions = new Set(selectedOptions.map((opt) => opt.value));
            const existingChildSelectedOptions =
                selectedOptions.filter((opt) => opt.parentValue === option.value) || [];
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
        } else if (isPartialSelected || (!isSelected && !areAnyChildrenSelected)) {
            const optionsToAdd = option.isParent && !areParentsSelectable ? selectableChildren : [option];
            addOptions(optionsToAdd);
        } else if (areAllChildrenSelected) {
            removeOptions([option, ...selectableChildren]);
        } else {
            handleOptionChange(option);
        }
    };

    // one loader variable for fetching data for expanded parents and their respective child nodes
    useEffect(() => {
        // once loading has been done just remove all the parent node urn
        if (!isLoadingParentChildList) {
            setLoadingParentUrns([]);
        }
    }, [isLoadingParentChildList]);

    return (
        <div>
            <ParentOption>
                <OptionLabel
                    key={option.value}
                    onClick={(e) => {
                        e.preventDefault();
                        if (isImplicitlySelected) {
                            return;
                        }
                        if (isParentMissingChildren) {
                            setLoadingParentUrns((previousIds) => [...previousIds, option.value]);
                            loadData?.(option);
                        }
                        if (option.isParent) {
                            setIsOpen(!isOpen);
                        } else {
                            selectOption();
                        }
                    }}
                    isSelected={!isMultiSelect && isSelected}
                    // added hack to show cursor in wait untill we get the inline spinner
                    style={{
                        width: '100%',
                        cursor: loadingParentUrns.includes(option.value) ? 'wait' : 'pointer',
                        display: 'flex',
                        justifyContent: hideParentCheckbox ? 'space-between' : 'normal',
                    }}
                >
                    {option.isParent && <strong>{option.label}</strong>}
                    {!option.isParent && <>{option.label}</>}
                    {option.isParent && (
                        <Icon
                            onClick={(e) => {
                                e.stopPropagation();
                                e.preventDefault();
                                setIsOpen(!isOpen);
                                if (!isOpen && isParentMissingChildren) {
                                    setLoadingParentUrns((previousIds) => [...previousIds, option.value]);
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
                    {!(hideParentCheckbox && option.isParent) && (
                        <StyledCheckbox
                            checked={isImplicitlySelected || isSelected}
                            indeterminate={
                                areParentsSelectable && option.isParent ? areAnyChildrenSelected : isPartialSelected
                            }
                            onClick={(e) => {
                                e.preventDefault();
                                if (isImplicitlySelected) {
                                    return;
                                }
                                e.stopPropagation();
                                if (isParentMissingChildren) {
                                    loadData?.(option);
                                    if (!areParentsSelectable) {
                                        setAutoSelectChildren(true);
                                    }
                                }
                                selectOption();
                            }}
                            disabled={isImplicitlySelected}
                        />
                    )}
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
                            setSelectedOptions={setSelectedOptions}
                        />
                    ))}
                </ChildOptions>
            )}
        </div>
    );
};
