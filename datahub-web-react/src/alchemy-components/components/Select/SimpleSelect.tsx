import { LoadingOutlined } from '@ant-design/icons';
import { Dropdown, Text } from '@components';
import { isEqual } from 'lodash';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
    ActionButtonsContainer,
    Container,
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
import { SelectOption, SelectProps } from '@components/components/Select/types';

import NoResultsFoundPlaceholder from '@app/searchV2/searchBarV2/components/NoResultsFoundPlaceholder';
import { LoadingWrapper } from '@src/app/entityV2/shared/tabs/Incident/AcrylComponents/styledComponents';

export const selectDefaults: SelectProps = {
    options: [],
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
    ignoreMaxHeight: false,
};

export const SimpleSelect = ({
    options = selectDefaults.options,
    label = selectDefaults.label,
    values,
    initialValues,
    onUpdate,
    onClear,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
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
    ...props
}: SelectProps) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues || values || []);
    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);
    const {
        isOpen,
        isVisible,
        close: closeDropdown,
        toggle: toggleDropdown,
    } = useSelectDropdown(false, selectRef, dropdownRef);
    const [areAllSelected, setAreAllSelected] = useState(false);

    useEffect(() => {
        if (values !== undefined && !isEqual(selectedValues, values)) {
            setSelectedValues(values);
        }
    }, [values, selectedValues]);

    useEffect(() => {
        setAreAllSelected(selectedValues.length === options.length);
    }, [options, selectedValues]);

    const filteredOptions = useMemo(
        () =>
            filterResultsByQuery
                ? options.filter((option) => option.label.toLowerCase().includes(searchQuery.toLowerCase()))
                : options,
        [options, searchQuery, filterResultsByQuery],
    );

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            toggleDropdown();
        }
    }, [toggleDropdown, isDisabled, isReadOnly]);

    const handleOptionChange = useCallback(
        (option: SelectOption) => {
            const updatedValues = selectedValues.includes(option.value)
                ? selectedValues.filter((val) => val !== option.value)
                : [...selectedValues, option.value];

            setSelectedValues(isMultiSelect ? updatedValues : [option.value]);
            if (onUpdate) {
                onUpdate(isMultiSelect ? updatedValues : [option.value]);
            }
            if (!isMultiSelect) closeDropdown();
        },
        [closeDropdown, onUpdate, isMultiSelect, selectedValues],
    );

    const handleClearSelection = useCallback(() => {
        setSelectedValues([]);
        setAreAllSelected(false);
        closeDropdown();
        if (onUpdate) {
            onUpdate([]);
        }
        if (onClear) {
            onClear();
        }
    }, [closeDropdown, onUpdate, onClear]);

    const handleSelectAll = () => {
        if (areAllSelected) {
            setSelectedValues([]);
            onUpdate?.([]);
        } else {
            const allValues = options.map((option) => option.value);
            setSelectedValues(allValues);
            onUpdate?.(allValues);
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
            $selectLabelVariant={selectLabelProps?.variant}
            isSelected={selectedValues.length > 0}
        >
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            {isVisible && (
                <Dropdown
                    open={isOpen}
                    disabled={isDisabled}
                    placement="bottomRight"
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
                            {isLoading ? (
                                <LoadingWrapper>
                                    <LoadingOutlined />
                                </LoadingWrapper>
                            ) : (
                                !filteredOptions.length && <NoResultsFoundPlaceholder />
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
                                {filteredOptions.map((option) => (
                                    <OptionLabel
                                        key={option.value}
                                        onClick={() => {
                                            if (!isMultiSelect) {
                                                if (optionSwitchable && selectedValues.includes(option.value)) {
                                                    handleClearSelection();
                                                } else {
                                                    handleOptionChange(option);
                                                }
                                            }
                                        }}
                                        isSelected={selectedValues.includes(option.value)}
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
                                                    onClick={() => handleOptionChange(option)}
                                                    checked={selectedValues.includes(option.value)}
                                                    disabled={disabledValues?.includes(option.value)}
                                                />
                                            </LabelContainer>
                                        ) : (
                                            <OptionContainer>
                                                {renderCustomOptionText ? (
                                                    renderCustomOptionText(option)
                                                ) : (
                                                    <ActionButtonsContainer>
                                                        {option.icon}
                                                        <Text
                                                            weight="semiBold"
                                                            size="md"
                                                            color={
                                                                selectedValues.includes(option.value)
                                                                    ? 'violet'
                                                                    : 'gray'
                                                            }
                                                        >
                                                            {option.label}
                                                        </Text>
                                                    </ActionButtonsContainer>
                                                )}

                                                {!!option.description && (
                                                    <Text color="gray" weight="normal" size="sm">
                                                        {option.description}
                                                    </Text>
                                                )}
                                            </OptionContainer>
                                        )}
                                    </OptionLabel>
                                ))}
                            </OptionList>
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
                        {...props}
                        position={position}
                        data-testid={dataTestId ? `${dataTestId}-base` : undefined}
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
                </Dropdown>
            )}
        </Container>
    );
};
