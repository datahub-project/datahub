import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Icon } from '@components';

import {
    ActionButtonsContainer,
    Container,
    OptionList,
    PortalDropdownContainer,
    SelectBase,
    SelectLabel,
    StyledClearButton,
} from '../components';

import { SelectLabelProps, SelectSizeOptions } from '../types';
import { NestedOption } from './NestedOption';
import { FilteringPredicate, SelectOption } from './types';
import Dropdown from '../../Dropdown/Dropdown';
import DropdownFooter from '../private/dropdown/DropdownFooter';
import SelectLabelRenderer from '../private/SelectLabelRenderer/SelectLabelRenderer';
import DropdownSearchBar from '../private/dropdown/DropdownSearchBar';
import { useOverlayClassStackContext } from '../../Utils/OverlayClassContext/OverlayClassContext';
import { areOptionsDifferent, filterNestedSelectOptions } from './utils';

const NO_PARENT_VALUE = 'no_parent_value';

export interface ActionButtonsProps {
    fontSize?: SelectSizeOptions;
    selectedOptions: SelectOption[];
    isOpen: boolean;
    isDisabled: boolean;
    isReadOnly: boolean;
    handleClearSelection: () => void;
    showCount?: boolean;
}

const SelectActionButtons = ({
    selectedOptions,
    isOpen,
    isDisabled,
    isReadOnly,
    handleClearSelection,
    fontSize = 'md',
    showCount = false,
}: ActionButtonsProps) => {
    return (
        <ActionButtonsContainer>
            {!showCount && !!selectedOptions.length && !isDisabled && !isReadOnly && (
                <StyledClearButton
                    icon="Close"
                    isCircle
                    onClick={handleClearSelection}
                    size={fontSize}
                    data-testid="dropdown-option-clear-icon"
                />
            )}
            <Icon icon="ChevronLeft" rotate={isOpen ? '90' : '270'} size="xl" color="gray" />
        </ActionButtonsContainer>
    );
};

export interface SelectProps<Option extends SelectOption = SelectOption> {
    options: Option[];
    label?: string;
    value?: string;
    initialValues?: Option[];
    onCancel?: () => void;
    onUpdate?: (selectedValues: Option[]) => void;
    shouldManuallyUpdate?: boolean;
    size?: SelectSizeOptions;
    showSearch?: boolean;
    shouldFilterOptions?: boolean;
    filteringPredicate?: FilteringPredicate<Option>;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    isMultiSelect?: boolean;
    areParentsSelectable?: boolean;
    loadData?: (node: Option) => void;
    onSearch?: (query: string) => void;
    width?: number | 'full' | 'fit-content';
    height?: number;
    placeholder?: string;
    searchPlaceholder?: string;
    isLoadingParentChildList?: boolean;
    showCount?: boolean;
    shouldAlwaysSyncParentValues?: boolean;
    hideParentCheckbox?: boolean;
    selectLabelProps?: SelectLabelProps;
}

export const selectDefaults: SelectProps = {
    options: [],
    label: '',
    size: 'md',
    showSearch: false,
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    isMultiSelect: false,
    width: 255,
    height: 425,
};

export const NestedSelect = ({
    options = selectDefaults.options,
    label = selectDefaults.label,
    initialValues = [],
    onUpdate,
    loadData,
    onSearch,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    isMultiSelect = selectDefaults.isMultiSelect,
    size = selectDefaults.size,
    areParentsSelectable = true,
    placeholder,
    searchPlaceholder,
    height = selectDefaults.height,
    isLoadingParentChildList = false,
    showCount = false,
    shouldAlwaysSyncParentValues = false,
    hideParentCheckbox = false,
    shouldManuallyUpdate = false,
    selectLabelProps,
    filteringPredicate,
    shouldFilterOptions,
    ...props
}: SelectProps) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const [selectedOptions, setSelectedOptions] = useState<SelectOption[]>(initialValues);
    const [stagedOptions, setStagedOptions] = useState<SelectOption[]>(initialValues);

    const overlayClassStack = useOverlayClassStackContext();
    const overlayClasses = useMemo(() => overlayClassStack.join(' '), [overlayClassStack]);

    useEffect(() => {
        if (initialValues && shouldAlwaysSyncParentValues) {
            // Check if selectedOptions and initialValues are different
            const areDifferent = areOptionsDifferent(selectedOptions, initialValues);

            if (initialValues && areDifferent) {
                setSelectedOptions(initialValues);
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [initialValues]);

    const handleSearch = useCallback(
        (query: string) => {
            setSearchQuery(query);
            onSearch?.(query);
        },
        [onSearch],
    );

    const filteredOptions = useMemo(() => {
        if (!shouldFilterOptions) return options;
        return filterNestedSelectOptions(options, searchQuery, filteringPredicate);
    }, [options, searchQuery, filteringPredicate, shouldFilterOptions]);

    // Instead of calling the update function individually whenever selectedOptions changes,
    // we use the useEffect hook to trigger the onUpdate function automatically when selectedOptions is updated.
    useEffect(() => {
        onUpdate?.(selectedOptions);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedOptions]);

    // Sync staged and selected options automaticly when shouldManuallyUpdate disabled
    useEffect(() => {
        if (!shouldManuallyUpdate) setSelectedOptions(stagedOptions);
    }, [shouldManuallyUpdate, stagedOptions]);

    const onClickUpdateButton = useCallback(() => {
        setSelectedOptions(stagedOptions); // update selected options
        setIsOpen(false);
    }, [stagedOptions]);

    const onClickCancelButton = useCallback(() => {
        setStagedOptions(selectedOptions); // reset staged options
        setIsOpen(false);
    }, [selectedOptions]);

    const handleOptionChange = useCallback(
        (option: SelectOption) => {
            let newSelectedOptions: SelectOption[];
            if (stagedOptions.find((o) => o.value === option.value)) {
                newSelectedOptions = stagedOptions.filter((o) => o.value !== option.value);
            } else {
                newSelectedOptions = [...stagedOptions, option];
            }
            setStagedOptions(newSelectedOptions);
            if (!isMultiSelect) {
                setIsOpen(false);
            }
        },
        [stagedOptions, isMultiSelect],
    );

    const addOptions = useCallback(
        (optionsToAdd: SelectOption[]) => {
            const existingValues = new Set(stagedOptions.map((option) => option.value));
            const filteredOptionsToAdd = optionsToAdd.filter((option) => !existingValues.has(option.value));
            if (filteredOptionsToAdd.length) {
                const newSelectedOptions = [...stagedOptions, ...filteredOptionsToAdd];
                setStagedOptions(newSelectedOptions);
            }
        },
        [stagedOptions],
    );

    const removeStagedOptions = useCallback(
        (optionsToRemove: SelectOption[], syncWithSelectedOptions?: boolean) => {
            const newValues = stagedOptions.filter(
                (selectedOption) => !optionsToRemove.find((o) => o.value === selectedOption.value),
            );
            setStagedOptions(newValues);
            if (!shouldManuallyUpdate || syncWithSelectedOptions) setSelectedOptions(newValues);
        },
        [stagedOptions, shouldManuallyUpdate],
    );

    const handleClearSelection = useCallback(() => {
        setStagedOptions([]);
        setSelectedOptions([]);
        setIsOpen(false);
        if (onUpdate) {
            onUpdate([]);
        }
    }, [onUpdate]);

    const onDropdownOpenChange = useCallback(
        (open: boolean) => {
            setIsOpen(open);

            // reset staged options on dropdown's closing when shouldManuallyUpdate enabled
            if (shouldManuallyUpdate && !open) {
                setStagedOptions(selectedOptions);
            }
        },
        [selectedOptions, shouldManuallyUpdate],
    );

    // generate map for options to quickly fetch children
    const parentValueToOptions: { [parentValue: string]: SelectOption[] } = {};
    filteredOptions.forEach((o) => {
        const parentValue = o.parentValue || NO_PARENT_VALUE;
        parentValueToOptions[parentValue] = parentValueToOptions[parentValue]
            ? [...parentValueToOptions[parentValue], o]
            : [o];
    });

    const rootOptions = parentValueToOptions[NO_PARENT_VALUE] || [];

    return (
        <Container size={size || 'md'} width={props.width || 255}>
            {label && <SelectLabel>{label}</SelectLabel>}
            <Dropdown
                open={isOpen}
                overlayClassName={overlayClasses}
                onOpenChange={onDropdownOpenChange}
                dropdownRender={() => {
                    return (
                        <PortalDropdownContainer style={{ maxHeight: height, overflow: 'auto' }}>
                            {showSearch && (
                                <DropdownSearchBar
                                    placeholder={searchPlaceholder}
                                    value={searchQuery}
                                    onChange={handleSearch}
                                    size={size}
                                />
                            )}
                            <OptionList>
                                {rootOptions.map((option) => (
                                    <NestedOption
                                        key={option.value}
                                        selectedOptions={stagedOptions}
                                        option={option}
                                        parentValueToOptions={parentValueToOptions}
                                        handleOptionChange={handleOptionChange}
                                        addOptions={addOptions}
                                        removeOptions={removeStagedOptions}
                                        loadData={loadData}
                                        isMultiSelect={isMultiSelect}
                                        setSelectedOptions={setStagedOptions}
                                        areParentsSelectable={areParentsSelectable}
                                        isLoadingParentChildList={isLoadingParentChildList}
                                        hideParentCheckbox={hideParentCheckbox}
                                    />
                                ))}
                            </OptionList>
                            {shouldManuallyUpdate && (
                                <DropdownFooter onUpdate={onClickUpdateButton} onCancel={onClickCancelButton} />
                            )}
                        </PortalDropdownContainer>
                    );
                }}
            >
                <SelectBase
                    isDisabled={isDisabled}
                    isReadOnly={isReadOnly}
                    isRequired={isRequired}
                    isOpen={isOpen}
                    fontSize={size}
                    data-testid="nested-options-dropdown-container"
                    width={props.width}
                    {...props}
                >
                    <SelectLabelRenderer
                        selectedValues={selectedOptions.map((o) => o.value)}
                        options={options}
                        placeholder={placeholder || 'Select an option'}
                        isMultiSelect={isMultiSelect}
                        removeOption={(option) => removeStagedOptions([option], true)}
                        {...(selectLabelProps || {})}
                    />
                    <SelectActionButtons
                        selectedOptions={selectedOptions}
                        isOpen={isOpen}
                        isDisabled={!!isDisabled}
                        isReadOnly={!!isReadOnly}
                        handleClearSelection={handleClearSelection}
                        fontSize={size}
                        showCount={showCount}
                    />
                </SelectBase>
            </Dropdown>
        </Container>
    );
};
