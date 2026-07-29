import { Dropdown, Text } from '@components';
import { isEqual } from 'lodash';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
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
import useSelectListboxKeyboard from '@components/components/Select/private/hooks/useSelectListboxKeyboard';
import { SelectOption, SelectProps } from '@components/components/Select/types';
import { getFooterButtonSize } from '@components/components/Select/utils';

let listboxIdCounter = 0;

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
    showSelectAll: false,
    showDescriptions: false,
};

export const BasicSelect = <OptionType extends SelectOption = SelectOption>({
    options = [],
    label = selectDefaults.label,
    values,
    initialValues,
    onCancel,
    onUpdate,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    showClear = selectDefaults.showClear,
    size = selectDefaults.size,
    isMultiSelect = selectDefaults.isMultiSelect,
    placeholder,
    disabledValues = [],
    showSelectAll = selectDefaults.showSelectAll,
    selectAllLabel,
    showDescriptions = selectDefaults.showDescriptions,
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
    const { t } = useTranslation('alchemy');
    const { t: tc } = useTranslation('common.actions');
    const resolvedSelectAllLabel = selectAllLabel ?? tc('selectAll');
    const generatedListboxId = useRef(`alchemy-basic-select-listbox-${listboxIdCounter++}`).current;
    const dropdownListId = dataTestId ? `${dataTestId}-listbox` : generatedListboxId;
    const [searchQuery, setSearchQuery] = useState('');
    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);
    const {
        isOpen,
        isVisible,
        open: openDropdown,
        close: closeDropdown,
        toggle: toggleDropdown,
    } = useSelectDropdown(false, selectRef, dropdownRef, visibilityDeps);

    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues || values || []);
    const [tempValues, setTempValues] = useState<string[]>(values || []);
    const [areAllSelected, setAreAllSelected] = useState(false);
    const [openSelectedValues, setOpenSelectedValues] = useState<string[]>([]);

    useEffect(() => {
        if (values !== undefined && !isEqual(selectedValues, values)) {
            setSelectedValues(values);
        }
    }, [values, selectedValues]);

    useEffect(() => {
        setAreAllSelected(tempValues.length === options.length);
    }, [options, tempValues]);

    useEffect(() => {
        if (isOpen) {
            setOpenSelectedValues(tempValues);
        }
    }, [isOpen]); // eslint-disable-line react-hooks/exhaustive-deps

    const filteredOptions = useMemo(() => {
        const filtered = options.filter((option) => option.label.toLowerCase().includes(searchQuery.toLowerCase()));

        if (!isMultiSelect || openSelectedValues.length === 0) return filtered;

        const selectedSet = new Set(openSelectedValues);
        return [...filtered].sort((a, b) => {
            const aSelected = selectedSet.has(a.value) ? 0 : 1;
            const bSelected = selectedSet.has(b.value) ? 0 : 1;
            return aSelected - bSelected;
        });
    }, [options, searchQuery, isMultiSelect, openSelectedValues]);

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            setTempValues(selectedValues);
            toggleDropdown();
        }
    }, [isDisabled, isReadOnly, selectedValues, toggleDropdown]);

    const openWithTempValues = useCallback(() => {
        setTempValues(selectedValues);
        openDropdown();
    }, [openDropdown, selectedValues]);

    const toggleWithTempValues = useCallback(() => {
        if (!isOpen) {
            setTempValues(selectedValues);
        }
        toggleDropdown();
    }, [isOpen, selectedValues, toggleDropdown]);

    const handleOptionChange = useCallback(
        (option: SelectOption) => {
            const updatedValues = tempValues.includes(option.value)
                ? tempValues.filter((val) => val !== option.value)
                : [...tempValues, option.value];

            setTempValues(isMultiSelect ? updatedValues : [option.value]);
        },
        [tempValues, isMultiSelect],
    );

    const handleClearSelection = useCallback(() => {
        setSelectedValues([]);
        setAreAllSelected(false);
        setTempValues([]);
        closeDropdown();
        if (onUpdate) {
            onUpdate([]);
        }
    }, [closeDropdown, onUpdate]);

    const {
        activeDescendantId,
        getOptionId,
        isOptionHighlighted,
        setHighlightedValue,
        onTriggerKeyDown: handleSelectKeyDown,
    } = useSelectListboxKeyboard({
        isOpen,
        isDisabled,
        isReadOnly,
        isMultiSelect,
        options: filteredOptions,
        disabledValues,
        selectedValues: tempValues,
        listboxId: dropdownListId,
        open: openWithTempValues,
        close: closeDropdown,
        toggle: toggleWithTempValues,
        onSelectOption: handleOptionChange,
        onClearSelection: handleClearSelection,
    });

    const removeOption = useCallback(
        (option: SelectOption) => {
            const updatedValues = selectedValues.filter((val) => val !== option.value);
            setSelectedValues(updatedValues);
            if (onUpdate) {
                onUpdate(updatedValues);
            }
        },
        [selectedValues, onUpdate],
    );

    const handleUpdateClick = useCallback(() => {
        setSelectedValues(tempValues);
        closeDropdown();
        if (onUpdate) {
            onUpdate(tempValues);
        }
    }, [closeDropdown, tempValues, onUpdate]);

    const handleCancelClick = useCallback(() => {
        closeDropdown();
        setTempValues(selectedValues);
        if (onCancel) {
            onCancel();
        }
    }, [closeDropdown, selectedValues, onCancel]);

    const handleSelectAll = () => {
        if (areAllSelected) {
            setTempValues([]);
            onUpdate?.([]);
        } else {
            const allValues = options.map((option) => option.value);
            setTempValues(allValues);
            onUpdate?.(allValues);
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
                                    placeholder={t('search.placeholder')}
                                    value={searchQuery}
                                    onChange={(value) => handleSearchChange(value)}
                                    size={size}
                                />
                            )}
                            <OptionList id={dropdownListId} role="listbox" aria-multiselectable={isMultiSelect}>
                                {showSelectAll && isMultiSelect && (
                                    <DropdownSelectAllOption
                                        label={resolvedSelectAllLabel}
                                        selected={areAllSelected}
                                        disabled={disabledValues.length === options.length}
                                        onClick={() => !(disabledValues.length === options.length) && handleSelectAll()}
                                    />
                                )}
                                {!filteredOptions.length && emptyState}
                                {filteredOptions.map((option) => {
                                    const isOptionDisabled = !!disabledValues?.includes(option.value);
                                    const isOptionSelected = tempValues.includes(option.value);
                                    const isHighlighted = isOptionHighlighted(option.value);
                                    return (
                                        <OptionLabel
                                            key={option.value}
                                            id={getOptionId(option.value)}
                                            onClick={() => {
                                                if (isOptionDisabled) return;
                                                handleOptionChange(option);
                                            }}
                                            onMouseEnter={() => {
                                                if (!isOptionDisabled) setHighlightedValue(option.value);
                                            }}
                                            tabIndex={-1}
                                            role="option"
                                            aria-selected={isOptionSelected}
                                            aria-disabled={isOptionDisabled}
                                            isSelected={isOptionSelected}
                                            isHighlighted={isHighlighted}
                                            isMultiSelect={isMultiSelect}
                                            isDisabled={isOptionDisabled}
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
                                                                    <span
                                                                        style={{ color: theme?.colors?.textTertiary }}
                                                                    >
                                                                        {option.description}
                                                                    </span>
                                                                </>
                                                            )}
                                                        </div>
                                                    )}
                                                    <span aria-hidden="true">
                                                        <StyledCheckbox
                                                            tabIndex={-1}
                                                            onCheckboxChange={() => handleOptionChange(option)}
                                                            isChecked={isOptionSelected}
                                                            isDisabled={isOptionDisabled}
                                                            size="sm"
                                                        />
                                                    </span>
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
                                                        <DescriptionContainer
                                                            style={{ maxWidth: descriptionMaxWidth }}
                                                        >
                                                            {option.description}
                                                        </DescriptionContainer>
                                                    )}
                                                </OptionContainer>
                                            )}
                                        </OptionLabel>
                                    );
                                })}
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
                        isOpen={isOpen}
                        onClick={handleSelectClick}
                        fontSize={size}
                        data-testid={dataTestId ? `${dataTestId}-base` : undefined}
                        {...props}
                        onKeyDown={handleSelectKeyDown}
                        role="combobox"
                        tabIndex={isDisabled || isReadOnly ? -1 : 0}
                        aria-haspopup="listbox"
                        aria-expanded={isOpen}
                        aria-controls={dropdownListId}
                        aria-activedescendant={activeDescendantId}
                        aria-disabled={isDisabled || isReadOnly}
                    >
                        <SelectLabelContainer>
                            {icon && <StyledIcon icon={icon} size="lg" />}
                            <SelectLabelRenderer
                                selectedValues={selectedValues}
                                options={options}
                                placeholder={placeholder || t('select.placeholder')}
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
