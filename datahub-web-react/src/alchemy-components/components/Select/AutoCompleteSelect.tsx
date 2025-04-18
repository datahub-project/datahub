import { Dropdown, Text } from '@components';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import {
    ActionButtonsContainer,
    Container,
    DropdownContainer,
    LabelsWrapper,
    OptionContainer,
    OptionLabel,
    OptionList,
    Placeholder,
    SelectBase,
    SelectLabel,
    SelectLabelContainer,
    StyledClearButton,
    StyledIcon,
} from '@components/components/Select/components';
import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';
import { ActionButtonsProps, SelectProps } from '@components/components/Select/types';

const NoSuggestions = styled.div`
    padding: 8px;
`;

const defaults: Partial<Props<any>> = {
    label: '',
    size: 'md',
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    width: 255,
    placeholder: 'Select an option ',
    disabledValues: [],
};

export interface Suggestion<T> {
    data: T;
    value: string;
}

type Props<T> = Pick<
    SelectProps,
    | 'label'
    | 'size'
    | 'isDisabled'
    | 'isReadOnly'
    | 'isRequired'
    | 'disabledValues'
    | 'width'
    | 'placeholder'
    | 'icon'
    | 'optionListTestId'
> & {
    render: (data: T) => React.ReactNode;
    emptySuggestions?: Suggestion<T>[];
    autoCompleteSuggestions?: Suggestion<T>[];
    onSearch: (query: string) => void;
    onUpdate?: (data: T | undefined) => void;
    searchPlaceholder?: string;
    name?: string;
    className?: string;
};

export default function AutoCompleteSelect<T>({
    render,
    emptySuggestions,
    autoCompleteSuggestions,
    onSearch,
    onUpdate,
    name,
    label = defaults.label,
    isDisabled = defaults.isDisabled,
    isReadOnly = defaults.isReadOnly,
    isRequired = defaults.isRequired,
    size = defaults.size,
    placeholder = defaults.placeholder,
    disabledValues = defaults.disabledValues,
    icon,
    searchPlaceholder,
    optionListTestId,
    className,
    ...props
}: Props<T>) {
    const [query, setQuery] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const [selectedValue, setSelectedValue] = useState<Suggestion<T> | undefined>(undefined);
    const selectRef = useRef<HTMLDivElement>(null);

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
        (option: Suggestion<T>) => {
            setSelectedValue(option);
            onUpdate?.(option.data);
            setIsOpen(false);
        },
        [onUpdate],
    );

    const handleClearSelection = useCallback(() => {
        setSelectedValue(undefined);
        setIsOpen(false);
        onUpdate?.(undefined);
    }, [onUpdate]);

    const isQuerySet = !!query;
    const [displayedSuggestions, setDisplayedSuggestions] = useState<Suggestion<T>[]>([]);
    useEffect(() => {
        if (isQuerySet && autoCompleteSuggestions) {
            setDisplayedSuggestions(autoCompleteSuggestions);
        } else if (!isQuerySet && emptySuggestions) {
            setDisplayedSuggestions(emptySuggestions);
        }
    }, [isQuerySet, autoCompleteSuggestions, emptySuggestions]);

    return (
        <Container
            ref={selectRef}
            className={className}
            size={size || 'md'}
            width={props.width || 255}
            isSelected={selectedValue !== undefined}
        >
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            <Dropdown
                open={isOpen}
                disabled={isDisabled}
                placement="bottomRight"
                dropdownRender={() => (
                    <DropdownContainer>
                        <DropdownSearchBar
                            placeholder={searchPlaceholder || ''}
                            value={query}
                            onChange={(value) => {
                                setQuery(value);
                                onSearch(value);
                            }}
                        />
                        <OptionList data-testid={optionListTestId}>
                            {!displayedSuggestions.length && (
                                <NoSuggestions>
                                    <Text type="span" color="gray" weight="semiBold">
                                        No results found
                                    </Text>
                                </NoSuggestions>
                            )}
                            {displayedSuggestions?.map((option) => (
                                <OptionLabel
                                    key={option.value}
                                    onClick={() => handleOptionChange(option)}
                                    isSelected={selectedValue?.value === option.value}
                                    isDisabled={disabledValues?.includes(option.value)}
                                >
                                    <OptionContainer>{render(option.data)}</OptionContainer>
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
                >
                    <SelectLabelContainer>
                        {icon && <StyledIcon icon={icon} size="lg" />}
                        <LabelsWrapper>
                            {!selectedValue && placeholder && <Placeholder>{placeholder}</Placeholder>}
                            {selectedValue && render(selectedValue.data)}
                        </LabelsWrapper>
                    </SelectLabelContainer>
                    <SelectActionButtons
                        selectedValues={selectedValue ? [selectedValue.value] : []}
                        isOpen={isOpen}
                        isDisabled={!!isDisabled}
                        isReadOnly={!!isReadOnly}
                        handleClearSelection={handleClearSelection}
                        showClear
                    />
                </SelectBase>
                <input type="hidden" name={name} value={selectedValue?.value || ''} readOnly />
            </Dropdown>
        </Container>
    );
}

function SelectActionButtons({
    selectedValues,
    isOpen,
    isDisabled,
    isReadOnly,
    showClear,
    handleClearSelection,
}: ActionButtonsProps) {
    return (
        <ActionButtonsContainer>
            {showClear && selectedValues.length > 0 && !isDisabled && !isReadOnly && (
                <StyledClearButton
                    icon={{ icon: 'Close', source: 'material', size: 'lg' }}
                    isCircle
                    onClick={handleClearSelection}
                />
            )}
            <StyledIcon icon="CaretDown" source="phosphor" rotate={isOpen ? '180' : '0'} size="md" color="gray" />
        </ActionButtonsContainer>
    );
}
