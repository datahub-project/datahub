import { Dropdown, Text } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTheme } from 'styled-components';

import {
    ActionButtonsContainer,
    Container,
    DescriptionContainer,
    DropdownContainer,
    LabelContainer,
    OptionContainer,
    OptionLabel,
    OptionList,
    SelectBase,
    SelectLabel,
    SelectLabelContainer,
    StyledCheckbox,
    StyledIcon,
} from '@components/components/Select/components';
import DropdownFooterActions from '@components/components/Select/private/DropdownFooterActions';
import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';
import DropdownSelectAllOption from '@components/components/Select/private/DropdownSelectAllOption';
import SelectActionButtons from '@components/components/Select/private/SelectActionButtons';
import SelectLabelRenderer from '@components/components/Select/private/SelectLabelRenderer/SelectLabelRenderer';
import useSelectDropdown from '@components/components/Select/private/hooks/useSelectDropdown';
import { useSelectionManagement } from '@components/components/Select/private/hooks/useSelectionManagement';
import { SelectOption, SelectProps } from '@components/components/Select/types';
import { getFooterButtonSize } from '@components/components/Select/utils';

// Updated main component
export const selectDefaults: SelectProps = {
    options: [],
    label: '',
    size: 'md',
    showSearch: false,
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    isMultiSelect: false,
    showClear: false,
    placeholder: 'Select an option',
    showSelectAll: false,
    selectAllLabel: 'Select All',
    showDescriptions: false,
};

export const BasicSelect = <OptionType extends SelectOption = SelectOption>({
    options = [],
    label = selectDefaults.label,
    values,
    initialValues,
    onCancel,
    onUpdate,
    onClose,
    shouldUpdateValuesOnClose,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    isActive,
    showClear = selectDefaults.showClear,
    size = selectDefaults.size,
    isMultiSelect = selectDefaults.isMultiSelect,
    placeholder = selectDefaults.placeholder,
    disabledValues = [],
    showSelectAll = selectDefaults.showSelectAll,
    selectAllLabel = selectDefaults.selectAllLabel,
    showDescriptions = selectDefaults.showDescriptions,
    shouldOrderSelectedOptionsToTop,
    icon,
    renderCustomOptionText,
    selectLabelProps,
    onSearchChange,
    emptyState,
    descriptionMaxWidth,
    dataTestId,
    visibilityDeps,
    ...props
}: SelectProps<OptionType>) => {
    const theme = useTheme();
    const [searchQuery, setSearchQuery] = useState('');
    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);

    const {
        selectedValues,
        stagedValues,
        setStagedValues,
        resetStagedValues,
        onValueChanged,
        clearSelection,
        commitSelection,
    } = useSelectionManagement({
        initialValues: initialValues || [],
        values,
        onUpdate,
        isMultiselect: isMultiSelect,
    });

    const handleDropdownClose = useCallback(() => {
        if (shouldUpdateValuesOnClose) {
            commitSelection();
        } else {
            resetStagedValues();
        }
        onClose?.();
    }, [commitSelection, onClose, shouldUpdateValuesOnClose, resetStagedValues]);

    const [areAllSelected, setAreAllSelected] = useState(false);

    const {
        isOpen,
        isVisible,
        close: closeDropdown,
        toggle: toggleDropdown,
    } = useSelectDropdown(false, selectRef, dropdownRef, visibilityDeps, handleDropdownClose);

    useEffect(() => {
        setAreAllSelected(stagedValues.length === options.length);
    }, [options, stagedValues]);

    const filteredOptions = useMemo(() => {
        const filtered = options.filter((option) => option.label.toLowerCase().includes(searchQuery.toLowerCase()));

        if (!isMultiSelect || stagedValues.length === 0) return filtered;

        if (!shouldOrderSelectedOptionsToTop) return filtered;

        const selectedSet = new Set(stagedValues);
        return [...filtered].sort((a, b) => {
            const aSelected = selectedSet.has(a.value) ? 0 : 1;
            const bSelected = selectedSet.has(b.value) ? 0 : 1;
            return aSelected - bSelected;
        });
    }, [options, searchQuery, isMultiSelect, shouldOrderSelectedOptionsToTop, stagedValues]);

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            toggleDropdown();
        }
    }, [isDisabled, isReadOnly, toggleDropdown]);

    const handleOptionChange = useCallback(
        (option: SelectOption) => {
            onValueChanged(option.value);
        },
        [onValueChanged],
    );

    const removeOption = useCallback(
        (option: SelectOption) => {
            const updatedValues = selectedValues.filter((val) => val !== option.value);
            setStagedValues(updatedValues, { autocommit: true });
        },
        [selectedValues, setStagedValues],
    );

    const handleUpdateClick = useCallback(() => {
        commitSelection();
        closeDropdown();
    }, [commitSelection, closeDropdown]);

    const handleCancelClick = useCallback(() => {
        resetStagedValues();
        closeDropdown();
        onCancel?.();
    }, [resetStagedValues, closeDropdown, onCancel]);

    const handleClearSelection = useCallback(() => {
        clearSelection({ autocommit: true });
        closeDropdown();
    }, [clearSelection, closeDropdown]);

    const handleSelectAll = () => {
        if (areAllSelected) {
            setStagedValues([], { autocommit: true });
        } else {
            const allValues = options.map((option) => option.value);
            setStagedValues(allValues, { autocommit: true });
        }
        setAreAllSelected(!areAllSelected);
    };

    const handleSearchChange = useCallback(
        (value: string) => {
            onSearchChange?.(value);
            setSearchQuery(value);
        },
        [onSearchChange],
    );

    return (
        <Container ref={selectRef} size={size || 'md'} width={props.width} $minWidth={props.minWidth}>
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            {isVisible && (
                <Dropdown
                    open={isOpen}
                    disabled={isDisabled}
                    placement="bottomRight"
                    dropdownRender={() => (
                        <DropdownContainer
                            ref={dropdownRef}
                            data-testid={dataTestId ? `${dataTestId}-dropdown` : undefined}
                        >
                            {showSearch && (
                                <DropdownSearchBar
                                    placeholder="Search…"
                                    value={searchQuery}
                                    onChange={(value) => handleSearchChange(value)}
                                    size={size}
                                />
                            )}
                            <OptionList>
                                {showSelectAll && isMultiSelect && (
                                    <DropdownSelectAllOption
                                        label={selectAllLabel}
                                        selected={areAllSelected}
                                        disabled={disabledValues.length === options.length}
                                        onClick={() => !(disabledValues.length === options.length) && handleSelectAll()}
                                    />
                                )}
                                {!filteredOptions.length && emptyState}
                                {filteredOptions.map((option) => (
                                    <OptionLabel
                                        key={option.value}
                                        onClick={() => !isMultiSelect && handleOptionChange(option)}
                                        isSelected={stagedValues.includes(option.value)}
                                        isMultiSelect={isMultiSelect}
                                        isDisabled={disabledValues?.includes(option.value)}
                                    >
                                        {isMultiSelect ? (
                                            <LabelContainer>
                                                {renderCustomOptionText ? (
                                                    renderCustomOptionText(option)
                                                ) : (
                                                    <div>
                                                        <span>{option.label}</span>
                                                        {!!option.description && (
                                                            <>
                                                                <br />
                                                                <span style={{ color: theme?.colors?.textTertiary }}>
                                                                    {option.description}
                                                                </span>
                                                            </>
                                                        )}
                                                    </div>
                                                )}
                                                <StyledCheckbox
                                                    onCheckboxChange={() => handleOptionChange(option)}
                                                    isChecked={stagedValues.includes(option.value)}
                                                    isDisabled={disabledValues?.includes(option.value)}
                                                    size="sm"
                                                />
                                            </LabelContainer>
                                        ) : (
                                            <OptionContainer>
                                                <ActionButtonsContainer>
                                                    {option.icon}
                                                    <Text weight="semiBold" size="md">
                                                        {option.label}
                                                    </Text>
                                                </ActionButtonsContainer>
                                                {!!option.description && (
                                                    <DescriptionContainer style={{ maxWidth: descriptionMaxWidth }}>
                                                        {option.description}
                                                    </DescriptionContainer>
                                                )}
                                            </OptionContainer>
                                        )}
                                    </OptionLabel>
                                ))}
                            </OptionList>
                            <DropdownFooterActions
                                onCancel={handleCancelClick}
                                onUpdate={handleUpdateClick}
                                size={getFooterButtonSize(size)}
                            />
                        </DropdownContainer>
                    )}
                >
                    <SelectBase
                        isDisabled={isDisabled}
                        isReadOnly={isReadOnly}
                        isRequired={isRequired}
                        isActive={isActive}
                        isOpen={isOpen}
                        onClick={handleSelectClick}
                        fontSize={size}
                        data-testid={dataTestId ? `${dataTestId}-base` : undefined}
                        {...props}
                    >
                        <SelectLabelContainer>
                            {icon && <StyledIcon icon={icon} size="lg" />}
                            <SelectLabelRenderer
                                selectedValues={selectedValues}
                                options={options}
                                placeholder={placeholder || 'Select an option'}
                                isMultiSelect={isMultiSelect}
                                removeOption={removeOption}
                                disabledValues={disabledValues}
                                showDescriptions={showDescriptions}
                                {...(selectLabelProps || {})}
                            />
                        </SelectLabelContainer>
                        <SelectActionButtons
                            hasSelectedValues={selectedValues.length > 0}
                            isOpen={isOpen}
                            isDisabled={!!isDisabled}
                            isReadOnly={!!isReadOnly}
                            handleClearSelection={handleClearSelection}
                            showClear={!!showClear}
                            fontSize={size}
                        />
                    </SelectBase>
                </Dropdown>
            )}
        </Container>
    );
};

export default BasicSelect;
