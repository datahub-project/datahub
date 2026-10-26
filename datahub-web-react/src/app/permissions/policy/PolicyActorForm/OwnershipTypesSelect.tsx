import { Icon, Text } from '@components';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import { SimpleSelect } from '@src/alchemy-components';
import { SelectOption } from '@src/alchemy-components/components/Select/types';

const PillContainer = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    height: 26px;
    padding: 0 8px;
    border-radius: 100em;
    border: 1px solid ${(props) => props.theme.colors.border};
    background-color: ${(props) => props.theme.colors.bg};
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};
    font-weight: 500;
    margin-top: 2px;
    margin-right: 2px;
`;

const CloseIcon = styled(Icon)`
    cursor: pointer;

    &:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

export interface OwnershipTypesSelectProps {
    ownershipTypes: any[];
    ownershipTypesSelectValue: string[];
    ownershipTypesMap: Record<string, any>;
    onSelectOwnershipTypeActor: (type: string) => void;
    onDeselectOwnershipTypeActor: (type: string) => void;
    onClearAll?: () => void;
    onPreventMouseDown?: (event: any) => void;
    placeholder: string;
}

export default function OwnershipTypesSelect({
    ownershipTypes,
    ownershipTypesSelectValue,
    ownershipTypesMap,
    onSelectOwnershipTypeActor,
    onDeselectOwnershipTypeActor,
    onClearAll,
    onPreventMouseDown,
    placeholder,
}: OwnershipTypesSelectProps) {
    const options = useMemo(() => {
        const typeOptions = ownershipTypes.map((type) => ({
            value: type.urn,
            label: type?.info?.name || type.urn,
        }));

        // A stored type missing from the current list would otherwise render no pill.
        const optionValues = new Set(typeOptions.map((option) => option.value));
        const orphanedOptions = ownershipTypesSelectValue
            .filter((urn) => !optionValues.has(urn))
            .map((urn) => ({ value: urn, label: ownershipTypesMap[urn] || urn }));

        return [...typeOptions, ...orphanedOptions];
    }, [ownershipTypes, ownershipTypesSelectValue, ownershipTypesMap]);

    const renderOption = useCallback(
        (option: SelectOption) => <Text size="sm">{ownershipTypesMap[option.value.toString()] || option.label}</Text>,
        [ownershipTypesMap],
    );

    const handleUpdate = useCallback(
        (next: string[]) => {
            const current = new Set(ownershipTypesSelectValue);
            const updated = new Set(next);

            // Find added items
            updated.forEach((item) => {
                if (!current.has(item)) {
                    onSelectOwnershipTypeActor(item);
                }
            });

            // Find removed items
            current.forEach((item) => {
                if (!updated.has(item)) {
                    onDeselectOwnershipTypeActor(item);
                }
            });
        },
        [ownershipTypesSelectValue, onSelectOwnershipTypeActor, onDeselectOwnershipTypeActor],
    );

    const renderSelectedValue = useCallback(
        (option: SelectOption) => (
            <PillContainer
                key={option.value}
                onMouseDown={onPreventMouseDown}
                onClick={(e: React.MouseEvent) => {
                    e.preventDefault();
                    e.stopPropagation();
                }}
            >
                <span>{ownershipTypesMap[option.value.toString()] || option.label}</span>
                <CloseIcon
                    icon={X}
                    size="sm"
                    color="gray"
                    onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        onDeselectOwnershipTypeActor(option.value as string);
                    }}
                />
            </PillContainer>
        ),
        [ownershipTypesMap, onPreventMouseDown, onDeselectOwnershipTypeActor],
    );

    return (
        <SimpleSelect
            isMultiSelect
            showSearch
            values={ownershipTypesSelectValue}
            onUpdate={handleUpdate}
            onClear={onClearAll}
            options={options}
            renderCustomOptionText={renderOption}
            renderCustomSelectedValue={renderSelectedValue}
            selectLabelProps={{ variant: 'custom' }}
            filterResultsByQuery
            placeholder={placeholder}
            width="full"
        />
    );
}
