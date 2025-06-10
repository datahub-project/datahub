import { LoadingOutlined } from '@ant-design/icons';
import { SelectOption } from '@components';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { Dropdown } from '@components/components/Dropdown';
import {
    DropdownContainer,
    LabelContainer,
    OptionLabel,
    OptionList,
    StyledCheckbox,
} from '@components/components/Select/components';
import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';
import useSelectDropdown from '@components/components/Select/private/hooks/useSelectDropdown';

import EntitySearchInputResultV2 from '@app/entityV2/shared/EntitySearchInput/EntitySearchInputResultV2';
import { LoadingWrapper } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/styledComponents';
import NoResultsFoundPlaceholder from '@app/searchV2/searchBarV2/components/NoResultsFoundPlaceholder';

import { Entity } from '@types';

const Container = styled.div`
    width: 'fit-content';
`;

interface OptionType extends SelectOption {
    entity?: Entity;
}

type Props = {
    options: OptionType[];
    children: React.ReactNode;
    isLoading?: boolean;
    values?: string[];
    onSearchChange?: (searchText: string) => void;
    onUpdate?: (selectedValues: string[]) => void;
    onClose?: () => void;
    onClearSearch?: () => void;
};

const EntitySelectFilter = ({
    options,
    children,
    isLoading,
    values = [],
    onSearchChange,
    onUpdate,
    onClose,
    onClearSearch,
}: Props) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [selectedValues, setSelectedValues] = useState<string[]>(values || []);

    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);
    const { isOpen, toggle: toggleDropdown } = useSelectDropdown(false, selectRef, dropdownRef, onClose);

    useEffect(() => {
        if (!isLoading) {
            setSelectedValues(values);
        }
    }, [values, isLoading, selectedValues]);

    const handleSearchChange = (value: string) => {
        onSearchChange?.(value);
        setSearchQuery(value);
    };

    const handleOptionChange = useCallback(
        (option: SelectOption) => {
            const updatedValues = selectedValues.includes(option.value)
                ? selectedValues.filter((val) => val !== option.value)
                : [...selectedValues, option.value];

            setSelectedValues(updatedValues);
            onUpdate?.(updatedValues);
        },
        [onUpdate, selectedValues],
    );

    const handleSelectClick = useCallback(() => {
        if (isOpen) {
            onClose?.();
        }
        toggleDropdown();
    }, [toggleDropdown, isOpen, onClose]);

    const handleClearSearch = () => {
        onClearSearch?.();
        setSearchQuery('');
    };

    const finalOptions = options;

    return (
        <Container ref={selectRef}>
            <Dropdown
                open={isOpen}
                disabled={false}
                placement="bottomLeft"
                dropdownRender={() => (
                    <DropdownContainer ref={dropdownRef} ignoreMaxHeight style={{ minWidth: '400px' }}>
                        <DropdownSearchBar
                            placeholder="Search…"
                            value={searchQuery}
                            onChange={(value) => handleSearchChange(value)}
                            size="sm"
                            onClear={handleClearSearch}
                        />
                        <OptionList
                            style={{
                                maxHeight: '30vh',
                                overflow: 'auto',
                            }}
                            data-testid="entity-select-options"
                        >
                            {finalOptions.map((option) => (
                                <OptionLabel key={option.value} isSelected={false} isMultiSelect>
                                    <LabelContainer>
                                        {option.entity && <EntitySearchInputResultV2 entity={option.entity} />}
                                        <StyledCheckbox
                                            onClick={() => handleOptionChange(option)}
                                            checked={selectedValues.includes(option.value)}
                                        />
                                    </LabelContainer>
                                </OptionLabel>
                            ))}
                        </OptionList>
                        {isLoading ? (
                            <LoadingWrapper>
                                <LoadingOutlined />
                            </LoadingWrapper>
                        ) : (
                            !finalOptions.length && (
                                <NoResultsFoundPlaceholder
                                    message={searchQuery?.length < 3 ? 'Type at least 3 characters..' : undefined}
                                />
                            )
                        )}
                    </DropdownContainer>
                )}
            >
                <div
                    onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            handleSelectClick();
                        }
                    }}
                    role="button"
                    tabIndex={0}
                    onClick={handleSelectClick}
                    style={{
                        outline: 'none',
                    }}
                >
                    {children}
                </div>
            </Dropdown>
        </Container>
    );
};

export default EntitySelectFilter;
