import React, { useState, useEffect } from 'react';

import { colors, Icon } from '@components';
import theme from '@components/theme';
import styled from 'styled-components';
import { Checkbox } from 'antd';

import { OptionLabel } from '../components';
import { SelectOption } from './types';
import useNestedOption from './useSelectOption';
import useNestedSelectOptionChildren from './useNestedSelectOptionChildren';

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
    isParentOptionLabelExpanded?: boolean;
    implicitlySelectChildren: boolean;
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
    isParentOptionLabelExpanded,
    implicitlySelectChildren,
}: OptionProps) => {
    const [loadingParentUrns, setLoadingParentUrns] = useState<string[]>([]);
    const [isOpen, setIsOpen] = useState(isParentOptionLabelExpanded);

    const { children, selectableChildren, directChildren, setAutoSelectChildren } = useNestedSelectOptionChildren({
        parentValueToOptions,
        option,
        areParentsSelectable,
        addOptions,
    });

    const { selectOption, isSelected, isImplicitlySelected, isPartialSelected, isParentMissingChildren } =
        useNestedOption({
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
        });

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
                        cursor:
                            isLoadingParentChildList && loadingParentUrns.includes(option.value) ? 'wait' : 'pointer',
                        display: 'flex',
                        justifyContent: hideParentCheckbox ? 'space-between' : 'normal',
                    }}
                    data-testid={`${option.isParent ? 'parent' : 'child'}-option-${option.value}`}
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
                            indeterminate={isPartialSelected}
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
                <ChildOptions data-testid="children-option-container">
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
                            implicitlySelectChildren={implicitlySelectChildren}
                        />
                    ))}
                </ChildOptions>
            )}
        </div>
    );
};
