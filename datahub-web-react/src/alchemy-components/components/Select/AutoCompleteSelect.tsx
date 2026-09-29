import { Dropdown, Loader, Text } from '@components';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import {
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
    StyledIcon,
} from '@components/components/Select/components';
import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';
import SelectActionButtons from '@components/components/Select/private/SelectActionButtons';
import useSelectDropdown from '@components/components/Select/private/hooks/useSelectDropdown';
import { SelectProps } from '@components/components/Select/types';

const NoSuggestions = styled.div`
    padding: 8px;
`;

const LoadingSuggestions = styled.div`
    display: flex;
    justify-content: center;
    padding: 8px;
`;

const defaults: Partial<Props<any>> = {
    label: '',
    size: 'md',
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    width: 255,
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
    | 'minWidth'
    | 'placeholder'
    | 'icon'
    | 'optionListTestId'
    | 'isLoading'
> & {
    render: (data: T) => React.ReactNode;
    /** Pre-selected suggestion. May resolve asynchronously, and is adopted until the user picks. */
    initialValue?: Suggestion<T>;
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
    initialValue,
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
    placeholder,
    disabledValues = defaults.disabledValues,
    icon,
    searchPlaceholder,
    optionListTestId,
    isLoading,
    className,
    ...props
}: Props<T>) {
    const { t } = useTranslation('alchemy');
    const { t: tc } = useTranslation('common.actions');
    const resolvedPlaceholder = placeholder ?? t('select.placeholder');
    const [query, setQuery] = useState('');
    const [selectedValue, setSelectedValue] = useState<Suggestion<T> | undefined>(initialValue);
    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);
    const {
        isOpen,
        isVisible,
        close: closeDropdown,
        toggle: toggleDropdown,
    } = useSelectDropdown(false, selectRef, dropdownRef);

    // Callers may only be able to resolve the pre-selection after a fetch, so adopt it when it
    // arrives — including a cleared one, hence the unconditional assignment. Once the user has
    // picked for themselves their choice wins, so a late-arriving initial value can't undo it.
    const hasUserPicked = useRef(false);
    useEffect(() => {
        if (!hasUserPicked.current) setSelectedValue(initialValue);
        // Keyed on the value rather than the object so a re-rendered caller changes nothing.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [initialValue?.value]);

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            toggleDropdown();
        }
    }, [toggleDropdown, isDisabled, isReadOnly]);

    const handleOptionChange = useCallback(
        (option: Suggestion<T>) => {
            // A disabled option is not a choice, so it neither selects nor counts as the user having
            // picked — otherwise it would also block a pre-selection that is still resolving.
            if (disabledValues?.includes(option.value)) return;
            hasUserPicked.current = true;
            setSelectedValue(option);
            onUpdate?.(option.data);
            closeDropdown();
        },
        [closeDropdown, onUpdate, disabledValues],
    );

    const handleClearSelection = useCallback(() => {
        hasUserPicked.current = true;
        setSelectedValue(undefined);
        closeDropdown();
        onUpdate?.(undefined);
    }, [closeDropdown, onUpdate]);

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
            $minWidth={props.minWidth}
            isSelected={selectedValue !== undefined}
        >
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            {isVisible && (
                <Dropdown
                    open={isOpen}
                    disabled={isDisabled}
                    placement="bottomRight"
                    dropdownRender={() => (
                        <DropdownContainer ref={dropdownRef}>
                            <DropdownSearchBar
                                placeholder={searchPlaceholder || ''}
                                value={query}
                                onChange={(value) => {
                                    setQuery(value);
                                    onSearch(value);
                                }}
                            />
                            <OptionList data-testid={optionListTestId}>
                                {/* Suggestions for an earlier keystroke are not shown as if they
                                    matched what is typed now: a click on one would pick the wrong
                                    entity. */}
                                {isLoading ? (
                                    <LoadingSuggestions>
                                        <Loader size="sm" />
                                    </LoadingSuggestions>
                                ) : (
                                    <>
                                        {!displayedSuggestions.length && (
                                            <NoSuggestions>
                                                <Text type="span" weight="semiBold">
                                                    {tc('noResults')}
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
                                    </>
                                )}
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
                                {!selectedValue && resolvedPlaceholder && (
                                    <Placeholder>{resolvedPlaceholder}</Placeholder>
                                )}
                                {selectedValue && render(selectedValue.data)}
                            </LabelsWrapper>
                        </SelectLabelContainer>
                        <SelectActionButtons
                            hasSelectedValues={!!selectedValue}
                            isOpen={isOpen}
                            isDisabled={!!isDisabled}
                            isReadOnly={!!isReadOnly}
                            handleClearSelection={handleClearSelection}
                            fontSize={size}
                            showClear
                        />
                        {/* Nested inside SelectBase because antd's Dropdown takes a single child. */}
                        <input type="hidden" name={name} value={selectedValue?.value || ''} readOnly />
                    </SelectBase>
                </Dropdown>
            )}
        </Container>
    );
}
