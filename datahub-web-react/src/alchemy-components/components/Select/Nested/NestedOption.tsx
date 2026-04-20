import { Icon } from '@components';
import { CaretLeft } from '@phosphor-icons/react/dist/csr/CaretLeft';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { Checkbox } from '@components/components/Checkbox';
import { NestedSelectOption } from '@components/components/Select/Nested/types';
import useNestedSelectOptionChildren from '@components/components/Select/Nested/useNestedSelectOptionChildren';
import useNestedOption from '@components/components/Select/Nested/useSelectOption';
import { OptionLabel } from '@components/components/Select/components';
import { CustomOptionRenderer } from '@components/components/Select/types';

const ParentOption = styled.div`
    display: flex;
    align-items: center;
`;

const ChildOptions = styled.div`
    padding-left: 20px;
`;

const CheckboxWrapper = styled.div`
    margin-left: auto;
`;

interface OptionProps<OptionType extends NestedSelectOption> {
    option: OptionType;
    selectedOptions: OptionType[];
    parentValueToOptions: { [parentValue: string]: OptionType[] };
    areParentsSelectable: boolean;
    handleOptionChange: (node: OptionType) => void;
    addOptions: (nodes: OptionType[]) => void;
    removeOptions: (nodes: OptionType[]) => void;
    loadData?: (node: OptionType) => void;
    isMultiSelect?: boolean;
    isLoadingParentChildList?: boolean;
    setSelectedOptions: React.Dispatch<React.SetStateAction<OptionType[]>>;
    hideParentCheckbox?: boolean;
    isParentOptionLabelExpanded?: boolean;
    implicitlySelectChildren: boolean;
    renderCustomOptionText?: CustomOptionRenderer<OptionType>;
}

export const NestedOption = <OptionType extends NestedSelectOption>({
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
    renderCustomOptionText,
}: OptionProps<OptionType>) => {
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
            isMultiSelect: !!isMultiSelect,
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
                    {renderCustomOptionText ? (
                        renderCustomOptionText(option)
                    ) : (
                        <>
                            {option.isParent && <strong>{option.label}</strong>}
                            {!option.isParent && <>{option.label}</>}
                        </>
                    )}
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
                            icon={CaretLeft}
                            rotate={isOpen ? '90' : '270'}
                            size="xl"
                            color="gray"
                            style={{ cursor: 'pointer', marginLeft: '4px' }}
                        />
                    )}
                    {!(hideParentCheckbox && option.isParent) && (
                        <CheckboxWrapper>
                            <Checkbox
                                isChecked={isImplicitlySelected || isSelected}
                                isIntermediate={isPartialSelected}
                                isDisabled={isImplicitlySelected}
                                size="sm"
                                onCheckboxChange={() => {
                                    if (isImplicitlySelected) {
                                        return;
                                    }
                                    if (isParentMissingChildren) {
                                        loadData?.(option);
                                        if (!areParentsSelectable) {
                                            setAutoSelectChildren(true);
                                        }
                                    }
                                    selectOption();
                                }}
                            />
                        </CheckboxWrapper>
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
                            renderCustomOptionText={renderCustomOptionText}
                        />
                    ))}
                </ChildOptions>
            )}
        </div>
    );
};
