import { Dropdown, Text } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { Loader } from '@components/components/Loader/Loader';
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
import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';
import DropdownSelectAllOption from '@components/components/Select/private/DropdownSelectAllOption';
import SelectActionButtons from '@components/components/Select/private/SelectActionButtons';
import SelectLabelRenderer from '@components/components/Select/private/SelectLabelRenderer/SelectLabelRenderer';
import useSelectDropdown from '@components/components/Select/private/hooks/useSelectDropdown';
import { useSelectionManagement } from '@components/components/Select/private/hooks/useSelectionManagement';
import { SelectOption, SelectProps } from '@components/components/Select/types';

import NoResultsFoundPlaceholder from '@app/searchV2/searchBarV2/components/NoResultsFoundPlaceholder';

export const selectDefaults: Partial<SelectProps> = {
    label: '',
    size: 'md',
    showSearch: false,
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    showClear: true,
    width: 255,
    isMultiSelect: false,
    placeholder: 'Select an option ',
    showSelectAll: false,
    selectAllLabel: 'Select All',
    showDescriptions: false,
    filterResultsByQuery: true,
    shouldOrderSelectedOptionsToTop: false,
    ignoreMaxHeight: false,
    autocommit: true,
};

export const SimpleSelect = <OptionType extends SelectOption = SelectOption>({
    options = [],
    label = selectDefaults.label,
    values,
    initialValues,
    onUpdate,
    onClear,
    onClose,
    shouldUpdateValuesOnClose,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    isActive,
    showClear = selectDefaults.showClear,
    size = selectDefaults.size,
    icon,
    isMultiSelect = selectDefaults.isMultiSelect,
    placeholder = selectDefaults.placeholder,
    disabledValues = [],
    showSelectAll = selectDefaults.showSelectAll,
    selectAllLabel = selectDefaults.selectAllLabel,
    showDescriptions = selectDefaults.showDescriptions,
    optionListTestId,
    renderCustomOptionText,
    renderCustomSelectedValue,
    filterResultsByQuery = selectDefaults.filterResultsByQuery,
    shouldOrderSelectedOptionsToTop = selectDefaults.shouldOrderSelectedOptionsToTop,
    onSearchChange,
    combinedSelectedAndSearchOptions,
    optionListStyle,
    optionSwitchable,
    selectLabelProps,
    selectedOptionListStyle,
    position,
    applyHoverWidth,
    ignoreMaxHeight = selectDefaults.ignoreMaxHeight,
    isLoading = false,
    dataTestId,
    visibilityDeps,
    placement = 'bottomLeft',
    renderSelectBase,
    renderOptionsFooter,
    emptyState,
    autocommit = selectDefaults.autocommit,
    ...props
}: SelectProps<OptionType>) => {
    const [searchQuery, setSearchQuery] = useState('');
    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);

    const {
        selectedValues,
        stagedValues,
        onValueChanged,
        clearSelection,
        commitSelection,
        setStagedValues,
        resetStagedValues,
    } = useSelectionManagement({
        initialValues: initialValues || values || [],
        values,
        onUpdate,
        isMultiselect: isMultiSelect,
        autocommit,
    });

    const handleDropdownClose = useCallback(() => {
        if (shouldUpdateValuesOnClose) {
            commitSelection();
        } else {
            resetStagedValues();
        }
        onClose?.();
    }, [commitSelection, onClose, shouldUpdateValuesOnClose, resetStagedValues]);

    const {
        isOpen,
        isVisible,
        close: closeDropdown,
        toggle: toggleDropdown,
    } = useSelectDropdown(false, selectRef, dropdownRef, visibilityDeps, handleDropdownClose);
    const [areAllSelected, setAreAllSelected] = useState(false);

    useEffect(() => {
        setAreAllSelected(selectedValues.length === options.length);
    }, [options, selectedValues]);

    const filteredOptions = useMemo(() => {
        const filtered = filterResultsByQuery
            ? options.filter((option) => option.label.toLowerCase().includes(searchQuery.toLowerCase()))
            : options;

        if (!isMultiSelect || selectedValues.length === 0) return filtered;

        if (!shouldOrderSelectedOptionsToTop) return filtered;

        const selectedValuesSet = new Set(selectedValues);
        return [...filtered].sort((a, b) => {
            const aValue = selectedValuesSet.has(a.value) ? 0 : 1;
            const bValue = selectedValuesSet.has(b.value) ? 0 : 1;
            return aValue - bValue;
        });
    }, [options, searchQuery, filterResultsByQuery, shouldOrderSelectedOptionsToTop, isMultiSelect, selectedValues]);

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            toggleDropdown();
        }
    }, [toggleDropdown, isDisabled, isReadOnly]);

    const handleOptionChange = useCallback(
        (option: SelectOption) => {
            onValueChanged(option.value);
            if (!isMultiSelect) closeDropdown();
        },
        [closeDropdown, isMultiSelect, onValueChanged],
    );

    const handleClearSelection = useCallback(() => {
        clearSelection({ autocommit: true });
        setAreAllSelected(false);
        closeDropdown();
        if (onClear) {
            onClear();
        }
    }, [clearSelection, closeDropdown, onClear]);

    const handleSelectAll = () => {
        if (areAllSelected) {
            clearSelection();
        } else {
            const allValues = options.map((option) => option.value);
            setStagedValues(allValues);
        }
        setAreAllSelected(!areAllSelected);
    };

    const handleSearchChange = (value: string) => {
        onSearchChange?.(value);
        setSearchQuery(value);
    };

    const finalOptions = combinedSelectedAndSearchOptions?.length ? combinedSelectedAndSearchOptions : options;

    return (
        <Container
            ref={selectRef}
            size={size || 'md'}
            width={props.width || 255}
            $minWidth={props.minWidth}
            $selectLabelVariant={selectLabelProps?.variant}
            isSelected={selectedValues.length > 0}
            data-testid={dataTestId}
        >
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            {isVisible && (
                <Dropdown
                    open={isOpen}
                    disabled={isDisabled}
                    placement={placement}
                    dropdownRender={() => (
                        <DropdownContainer
                            ref={dropdownRef}
                            ignoreMaxHeight={ignoreMaxHeight}
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
                            <OptionList style={optionListStyle} data-testid={optionListTestId}>
                                {showSelectAll && isMultiSelect && (
                                    <DropdownSelectAllOption
                                        label={selectAllLabel}
                                        selected={areAllSelected}
                                        disabled={disabledValues.length === options.length}
                                        onClick={() => !(disabledValues.length === options.length) && handleSelectAll()}
                                    />
                                )}
                                {isLoading ? (
                                    <Loader size="sm" />
                                ) : (
                                    !filteredOptions.length && (emptyState ?? <NoResultsFoundPlaceholder />)
                                )}
                                {filteredOptions.map((option) => (
                                    <OptionLabel
                                        key={option.value}
                                        data-testid={`option-${option.value}`}
                                        onClick={() => {
                                            const isOptionDisabled = !!disabledValues?.includes(option.value);
                                            if (!isOptionDisabled && !isMultiSelect) {
                                                if (optionSwitchable && stagedValues.includes(option.value)) {
                                                    handleClearSelection();
                                                } else {
                                                    handleOptionChange(option);
                                                }
                                            }
                                        }}
                                        isSelected={stagedValues.includes(option.value)}
                                        isMultiSelect={isMultiSelect}
                                        isDisabled={disabledValues?.includes(option.value)}
                                        applyHoverWidth={applyHoverWidth}
                                    >
                                        {isMultiSelect ? (
                                            <LabelContainer>
                                                {renderCustomOptionText ? (
                                                    renderCustomOptionText(option)
                                                ) : (
                                                    <span>{option.label}</span>
                                                )}
                                                <StyledCheckbox
                                                    onCheckboxChange={() => handleOptionChange(option)}
                                                    isChecked={stagedValues.includes(option.value)}
                                                    isDisabled={disabledValues?.includes(option.value)}
                                                    dataTestId={`option-${option.value}-checkbox`}
                                                    size="sm"
                                                />
                                            </LabelContainer>
                                        ) : (
                                            <OptionContainer>
                                                {renderCustomOptionText ? (
                                                    renderCustomOptionText(option)
                                                ) : (
                                                    <ActionButtonsContainer>
                                                        {option.icon}
                                                        <Text weight="semiBold" size="md">
                                                            {option.label}
                                                        </Text>
                                                    </ActionButtonsContainer>
                                                )}

                                                {!!option.description && (
                                                    <DescriptionContainer
                                                        style={{ maxWidth: props.descriptionMaxWidth }}
                                                    >
                                                        {option.description}
                                                    </DescriptionContainer>
                                                )}
                                            </OptionContainer>
                                        )}
                                    </OptionLabel>
                                ))}
                                {renderOptionsFooter?.()}
                            </OptionList>
                        </DropdownContainer>
                    )}
                >
                    {renderSelectBase ? (
                        renderSelectBase({
                            isOpened: isOpen,
                            onClick: handleSelectClick,
                        })
                    ) : (
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
                            position={position}
                        >
                            <SelectLabelContainer>
                                {icon && <StyledIcon icon={icon} size="lg" />}
                                <SelectLabelRenderer
                                    selectedValues={selectedValues}
                                    options={finalOptions}
                                    placeholder={placeholder || 'Select an option'}
                                    isMultiSelect={isMultiSelect}
                                    removeOption={handleOptionChange}
                                    disabledValues={disabledValues}
                                    showDescriptions={showDescriptions}
                                    renderCustomSelectedValue={renderCustomSelectedValue}
                                    selectedOptionListStyle={selectedOptionListStyle}
                                    {...(selectLabelProps || {})}
                                />
                            </SelectLabelContainer>
                            <SelectActionButtons
                                hasSelectedValues={selectedValues.length > 0}
                                isOpen={isOpen}
                                isDisabled={!!isDisabled}
                                isReadOnly={!!isReadOnly}
                                handleClearSelection={handleClearSelection}
                                fontSize={size}
                                showClear={!!showClear}
                            />
                        </SelectBase>
                    )}
                </Dropdown>
            )}
        </Container>
    );
};
