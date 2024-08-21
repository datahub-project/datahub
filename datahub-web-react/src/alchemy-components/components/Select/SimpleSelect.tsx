import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';

import { Icon } from '@components';

import {
    SelectBase,
    OptionList,
    OptionLabel,
    Container,
    Dropdown,
    SearchInputContainer,
    SearchInput,
    SelectLabel,
    SearchIcon,
    SelectValue,
    Placeholder,
    ActionButtonsContainer,
    StyledClearButton,
} from './components';

import { SelectProps, SelectOption, ActionButtonsProps, SelectLabelDisplayProps } from './types';

const SelectLabelDisplay = ({ selectedValue, options, placeholder }: SelectLabelDisplayProps) => {
    const selectedOption = options.find((opt) => opt.value === selectedValue);
    return (
        <div>
            {selectedOption ? (
                <SelectValue>{selectedOption.label}</SelectValue>
            ) : (
                <Placeholder>{placeholder}</Placeholder>
            )}
        </div>
    );
};

const SelectActionButtons = ({
    selectedValue,
    isOpen,
    isDisabled,
    isReadOnly,
    handleClearSelection,
    fontSize = 'md',
}: ActionButtonsProps) => {
    return (
        <ActionButtonsContainer>
            {selectedValue && !isDisabled && !isReadOnly && (
                <StyledClearButton icon="Close" isCircle onClick={handleClearSelection} size={fontSize} />
            )}
            <Icon icon="ChevronLeft" rotate={isOpen ? '90' : '270'} size="xl" color="gray" />
        </ActionButtonsContainer>
    );
};

export const selectDefaults: SelectProps = {
    options: [],
    label: '',
    size: 'md',
    showSearch: false,
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    width: 255,
};

export const SimpleSelect = ({
    options = selectDefaults.options,
    label = selectDefaults.label,
    value = '',
    onUpdate,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    size = selectDefaults.size,
    ...props
}: SelectProps) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const [selectedValue, setSelectedValue] = useState<string>(value);
    const selectRef = useRef<HTMLDivElement>(null);

    const filteredOptions = useMemo(
        () => options.filter((option) => option.label.toLowerCase().includes(searchQuery.toLowerCase())),
        [options, searchQuery],
    );

    const handleDocumentClick = useCallback((e: MouseEvent) => {
        if (selectRef.current && !selectRef.current.contains(e.target as Node)) {
            setIsOpen(false);
        }
    }, []);

    useEffect(() => {
        document.addEventListener('click', handleDocumentClick);
        return () => {
            document.removeEventListener('click', handleDocumentClick);
        };
    }, [handleDocumentClick]);

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            setIsOpen((prev) => !prev);
        }
    }, [isDisabled, isReadOnly]);

    const handleOptionChange = useCallback(
        (option: SelectOption) => {
            setSelectedValue(option.value);
            setIsOpen(false);
            if (onUpdate) {
                onUpdate([option.value]);
            }
        },
        [onUpdate],
    );

    const handleClearSelection = useCallback(() => {
        setSelectedValue('');
        setIsOpen(false);
        if (onUpdate) {
            onUpdate([]);
        }
    }, [onUpdate]);

    return (
        <Container ref={selectRef} size={size || 'md'} width={props.width || 255}>
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            <SelectBase
                isDisabled={isDisabled}
                isReadOnly={isReadOnly}
                isRequired={isRequired}
                isOpen={isOpen}
                onClick={handleSelectClick}
                fontSize={size}
                {...props}
            >
                <SelectLabelDisplay selectedValue={selectedValue} options={options} placeholder="Select an option" />
                <SelectActionButtons
                    selectedValue={selectedValue}
                    isOpen={isOpen}
                    isDisabled={!!isDisabled}
                    isReadOnly={!!isReadOnly}
                    handleClearSelection={handleClearSelection}
                    fontSize={size}
                />
            </SelectBase>
            {isOpen && (
                <Dropdown>
                    {showSearch && (
                        <SearchInputContainer>
                            <SearchInput
                                type="text"
                                placeholder="Search…"
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                                style={{ fontSize: size || 'md' }}
                            />
                            <SearchIcon icon="Search" size={size} color="gray" />
                        </SearchInputContainer>
                    )}
                    <OptionList>
                        {filteredOptions.map((option) => (
                            <OptionLabel
                                key={option.value}
                                onClick={() => handleOptionChange(option)}
                                isSelected={option.value === selectedValue}
                            >
                                {option.label}
                            </OptionLabel>
                        ))}
                    </OptionList>
                </Dropdown>
            )}
        </Container>
    );
};
