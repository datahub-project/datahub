import { Dropdown } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { NestedOption } from '@components/components/Select/Nested/NestedOption';
import { NestedSelectOption, RenderOptionProps } from '@components/components/Select/Nested/types';
import { filterNestedSelectOptions } from '@components/components/Select/Nested/utils';
import {
    Container,
    DropdownContainer,
    OptionList,
    SelectBase,
    SelectLabel,
} from '@components/components/Select/components';
import DropdownFooterActions from '@components/components/Select/private/DropdownFooterActions';
import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';
import SelectActionButtons from '@components/components/Select/private/SelectActionButtons';
import SelectLabelRenderer from '@components/components/Select/private/SelectLabelRenderer/SelectLabelRenderer';
import useSelectDropdown from '@components/components/Select/private/hooks/useSelectDropdown';
import { useSelectionManagement } from '@components/components/Select/private/hooks/useSelectionManagement';
import { CustomOptionRenderer, SelectLabelProps, SelectSizeOptions } from '@components/components/Select/types';
import { getFooterButtonSize } from '@components/components/Select/utils';

const NO_PARENT_VALUE = 'no_parent_value';

export interface SelectProps<OptionType extends NestedSelectOption = NestedSelectOption> {
    options: OptionType[];
    label?: string;
    value?: string;
    values?: string[];
    initialValues?: OptionType[];
    onCancel?: () => void;
    onUpdate?: (selectedValues: OptionType[]) => void;
    onClear?: () => void;
    onClose?: () => void;
    shouldUpdateValuesOnClose?: boolean;
    onToggle?: (isOpen: boolean) => void;
    size?: SelectSizeOptions;
    showSearch?: boolean;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    isActive?: boolean;
    isMultiSelect?: boolean;
    areParentsSelectable?: boolean;
    loadData?: (node: OptionType) => void;
    onSearch?: (query: string) => void;
    width?: number | 'full' | 'fit-content';
    minWidth?: string;
    placeholder?: string;
    searchPlaceholder?: string;
    isLoadingParentChildList?: boolean;
    showClear?: boolean;
    shouldAlwaysSyncParentValues?: boolean;
    hideParentCheckbox?: boolean;
    implicitlySelectChildren?: boolean;
    shouldDisplayConfirmationFooter?: boolean;
    autocommit?: boolean;
    selectLabelProps?: SelectLabelProps;
    renderCustomOptionText?: CustomOptionRenderer<OptionType>;
    dataTestId?: string;
    ignoreMaxHeight?: boolean;
    visibilityDeps?: React.DependencyList;
    renderCustomOption?: (props: RenderOptionProps<OptionType>) => React.ReactNode;
}

export const selectDefaults: SelectProps = {
    options: [],
    label: '',
    size: 'md',
    showSearch: false,
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    isMultiSelect: true,
    width: 255,
    shouldDisplayConfirmationFooter: false,
    autocommit: false,
    showClear: true,
};

export const NestedSelect = <OptionType extends NestedSelectOption = NestedSelectOption>({
    options = [],
    label = selectDefaults.label,
    values,
    initialValues = [],
    onUpdate,
    onClear,
    onClose,
    shouldUpdateValuesOnClose,
    loadData,
    onSearch,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    isActive,
    showClear = selectDefaults.showClear,
    size = selectDefaults.size,
    isMultiSelect = selectDefaults.isMultiSelect,
    areParentsSelectable = true,
    placeholder,
    searchPlaceholder,
    isLoadingParentChildList = false,
    shouldAlwaysSyncParentValues = false,
    hideParentCheckbox = false,
    implicitlySelectChildren = true,
    shouldDisplayConfirmationFooter = selectDefaults.shouldDisplayConfirmationFooter,
    autocommit = selectDefaults.autocommit,
    selectLabelProps,
    renderCustomOptionText,
    dataTestId,
    ignoreMaxHeight = false,
    visibilityDeps,
    renderCustomOption,
    ...props
}: SelectProps<OptionType>) => {
    const [searchQuery, setSearchQuery] = useState('');
    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);

    const initialValueStrings = useMemo(() => initialValues?.map((v) => v.value) || [], [initialValues]);
    const {
        selectedValues,
        stagedValues,
        setSelectedValues,
        setStagedValues,
        onValueChanged,
        clearSelection,
        commitSelection,
        resetStagedValues,
    } = useSelectionManagement({
        initialValues: initialValueStrings,
        values,
        onUpdate: (newValues) => {
            const newOptions = options.filter((opt) => newValues.includes(opt.value));
            onUpdate?.(newOptions);
        },
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
    }, [shouldUpdateValuesOnClose, resetStagedValues, commitSelection, onClose]);

    const {
        isOpen,
        isVisible,
        close: closeDropdown,
        toggle: toggleDropdown,
    } = useSelectDropdown(false, selectRef, dropdownRef, visibilityDeps, handleDropdownClose);

    // Convert string values to options
    const selectedOptions = useMemo(
        () => options.filter((opt) => selectedValues.includes(opt.value)),
        [options, selectedValues],
    );

    const stagedOptions = useMemo(
        () => options.filter((opt) => stagedValues.includes(opt.value)),
        [options, stagedValues],
    );

    useEffect(() => {
        if (initialValues && shouldAlwaysSyncParentValues) {
            // Check if selectedOptions and initialValues are different
            const areDifferent = JSON.stringify(selectedOptions) !== JSON.stringify(initialValues);

            if (initialValues && areDifferent) {
                setSelectedValues(initialValues.map((option) => option.value));
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [setSelectedValues, initialValues, shouldAlwaysSyncParentValues]);

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            toggleDropdown();
        }
    }, [toggleDropdown, isDisabled, isReadOnly]);

    const handleSearch = useCallback(
        (query: string) => {
            setSearchQuery(query);
            onSearch?.(query);
        },
        [onSearch],
    );

    const filteredOptions = useMemo(() => {
        return filterNestedSelectOptions(options, searchQuery);
    }, [options, searchQuery]);

    const onClickUpdateButton = useCallback(() => {
        commitSelection();
        closeDropdown();
        handleSearch('');
    }, [commitSelection, closeDropdown, handleSearch]);

    const onClickCancelButton = useCallback(() => {
        // Reset staged values to selected values
        resetStagedValues();
        closeDropdown();
        handleSearch('');
    }, [resetStagedValues, closeDropdown, handleSearch]);

    const handleOptionChange = useCallback(
        (option: OptionType) => {
            onValueChanged(option.value);
            if (!isMultiSelect) {
                closeDropdown();
            }
        },
        [closeDropdown, isMultiSelect, onValueChanged],
    );

    const addOptions = useCallback(
        (optionsToAdd: OptionType[]) => {
            if (isMultiSelect) {
                const newValues = [
                    ...stagedValues,
                    ...optionsToAdd.map((option) => option.value).filter((value) => !stagedValues.includes(value)),
                ];

                setStagedValues(newValues);
            } else {
                const newValue = optionsToAdd?.[0]?.value;
                if (newValue) setStagedValues([newValue]);
            }
        },
        [stagedValues, setStagedValues, isMultiSelect],
    );

    const removeOptions = useCallback(
        (optionsToRemove: OptionType[], autocommitFlag?: boolean) => {
            const valuesToRemove = optionsToRemove.map((option) => option.value);
            const newValues = stagedValues.filter((value) => !valuesToRemove.includes(value));
            setStagedValues(newValues, { autocommit: autocommitFlag });
        },
        [stagedValues, setStagedValues],
    );

    const handleClearSelection = useCallback(() => {
        clearSelection({ autocommit: true });
        closeDropdown();
        if (onClear) {
            onClear();
        }
    }, [clearSelection, closeDropdown, onClear]);

    const setStagedOptions = useCallback(
        (newStagedOptions: OptionType[]) => {
            const newStagedValues = newStagedOptions.map((opt) => opt.value);
            setStagedValues(newStagedValues);
        },
        [setStagedValues],
    );

    // generate map for options to quickly fetch children
    const parentValueToOptions: { [parentValue: string]: OptionType[] } = {};
    filteredOptions.forEach((o) => {
        const parentValue = o.parentValue || NO_PARENT_VALUE;
        parentValueToOptions[parentValue] = parentValueToOptions[parentValue]
            ? [...parentValueToOptions[parentValue], o]
            : [o];
    });

    const rootOptions = parentValueToOptions[NO_PARENT_VALUE] || [];

    return (
        <Container
            ref={selectRef}
            size={size || 'md'}
            width={props.width || 255}
            $minWidth={props.minWidth}
            $selectLabelVariant={selectLabelProps?.variant}
            isSelected={selectedOptions.length > 0}
            data-testid={dataTestId}
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
                                    placeholder={searchPlaceholder}
                                    value={searchQuery}
                                    onChange={(value) => handleSearch(value)}
                                    size={size}
                                />
                            )}
                            <OptionList>
                                {rootOptions.map((option) => {
                                    const isParentOptionLabelExpanded = stagedOptions.find(
                                        (opt) => opt.parentValue === option.value,
                                    );
                                    return (
                                        <NestedOption
                                            key={option.value}
                                            selectedOptions={stagedOptions}
                                            option={option}
                                            parentValueToOptions={parentValueToOptions}
                                            handleOptionChange={handleOptionChange}
                                            addOptions={addOptions}
                                            removeOptions={removeOptions}
                                            loadData={loadData}
                                            isMultiSelect={isMultiSelect}
                                            setSelectedOptions={setStagedOptions}
                                            areParentsSelectable={areParentsSelectable}
                                            isLoadingParentChildList={isLoadingParentChildList}
                                            hideParentCheckbox={hideParentCheckbox}
                                            isParentOptionLabelExpanded={!!isParentOptionLabelExpanded}
                                            implicitlySelectChildren={implicitlySelectChildren}
                                            renderCustomOptionText={renderCustomOptionText}
                                            renderCustomOption={renderCustomOption}
                                        />
                                    );
                                })}
                            </OptionList>
                            {shouldDisplayConfirmationFooter && (
                                <DropdownFooterActions
                                    onUpdate={onClickUpdateButton}
                                    onCancel={onClickCancelButton}
                                    size={getFooterButtonSize(size)}
                                />
                            )}
                        </DropdownContainer>
                    )}
                >
                    <SelectBase
                        isDisabled={isDisabled}
                        isReadOnly={isReadOnly}
                        isRequired={isRequired}
                        isOpen={isOpen}
                        isActive={isActive}
                        onClick={handleSelectClick}
                        fontSize={size}
                        data-testid={dataTestId ? `${dataTestId}-base` : undefined}
                        width={props.width}
                        {...props}
                    >
                        <SelectLabelRenderer
                            selectedValues={selectedValues}
                            options={options}
                            placeholder={placeholder || 'Select an option'}
                            isMultiSelect={isMultiSelect}
                            removeOption={(option) => removeOptions([option], true)}
                            {...(selectLabelProps || {})}
                        />
                        <SelectActionButtons
                            hasSelectedValues={selectedValues.length > 0}
                            isOpen={isOpen}
                            isDisabled={!!isDisabled}
                            isReadOnly={!!isReadOnly}
                            handleClearSelection={handleClearSelection}
                            fontSize={size}
                            showClear={showClear}
                        />
                    </SelectBase>
                </Dropdown>
            )}
        </Container>
    );
};
