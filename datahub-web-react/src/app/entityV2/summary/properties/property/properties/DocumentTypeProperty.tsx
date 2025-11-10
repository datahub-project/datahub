import React, { useState } from 'react';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/documentV2/hooks/useDocumentPermissions';
import { useUpdateDocument } from '@app/documentV2/hooks/useUpdateDocument';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { Button, Input, SimpleSelect } from '@src/alchemy-components';

import { Document } from '@types';

const TypeSelectWrapper = styled.div`
    overflow: hidden;
`;

const CustomInputWrapper = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

const CUSTOM_VALUE = '__CUSTOM__';

const predefinedTypes = [
    { label: 'Runbook', value: 'Runbook' },
    { label: 'FAQ', value: 'FAQ' },
    { label: 'Insight', value: 'Insight' },
    { label: 'Definition', value: 'Definition' },
    { label: 'Decision', value: 'Decision' },
    { label: 'Custom', value: CUSTOM_VALUE },
];

export default function DocumentTypeProperty(props: PropertyComponentProps) {
    const { urn, entityData } = useEntityData();
    const document = entityData as Document;
    const refetch = useRefetch();
    const { canEdit } = useDocumentPermissions(urn);
    const { updateSubType } = useUpdateDocument();

    const currentType = document?.subType || '';
    const [isCustomMode, setIsCustomMode] = useState(false);
    const [customValue, setCustomValue] = useState('');

    const isPredefinedType = predefinedTypes.some((type) => type.value === currentType);

    const handleTypeChange = async (values: string[]) => {
        const selectedValue = values[0];

        if (selectedValue === CUSTOM_VALUE) {
            // Switch to custom input mode
            setIsCustomMode(true);
            setCustomValue(currentType);
        } else {
            // Update with predefined type using dedicated mutation
            console.log('[DocumentTypeProperty] Updating type to:', selectedValue);
            await updateSubType({
                urn,
                subType: selectedValue,
            });
            console.log('[DocumentTypeProperty] Type updated, calling refetch...');
            await refetch();
            console.log('[DocumentTypeProperty] Refetch complete!');
        }
    };

    const handleCustomSubmit = async () => {
        if (customValue.trim()) {
            console.log('[DocumentTypeProperty] Updating custom type to:', customValue.trim());
            await updateSubType({
                urn,
                subType: customValue.trim(),
            });
            setIsCustomMode(false);
            console.log('[DocumentTypeProperty] Type updated, calling refetch...');
            await refetch();
            console.log('[DocumentTypeProperty] Refetch complete!');
        }
    };

    const handleCustomCancel = () => {
        setIsCustomMode(false);
        setCustomValue('');
    };

    const renderValue = () => {
        if (!currentType) {
            return (
                <TypeSelectWrapper>
                    <SimpleSelect
                        values={[]}
                        onUpdate={handleTypeChange}
                        isDisabled={!canEdit}
                        options={predefinedTypes}
                        size="sm"
                        placeholder="Select type..."
                        width="fit-content"
                        showClear={false}
                    />
                </TypeSelectWrapper>
            );
        }

        if (isCustomMode) {
            return (
                <CustomInputWrapper>
                    <Input
                        value={customValue}
                        setValue={setCustomValue}
                        label=""
                        placeholder="Enter custom type"
                        autoFocus
                        onKeyDown={(e) => {
                            if (e.key === 'Enter') {
                                handleCustomSubmit();
                            } else if (e.key === 'Escape') {
                                handleCustomCancel();
                            }
                        }}
                    />
                    <Button onClick={handleCustomSubmit} size="sm">
                        Save
                    </Button>
                    <Button onClick={handleCustomCancel} size="sm" variant="text">
                        Cancel
                    </Button>
                </CustomInputWrapper>
            );
        }

        // Show current type with Select dropdown
        const currentValue = isPredefinedType ? currentType : CUSTOM_VALUE;
        return (
            <TypeSelectWrapper>
                <SimpleSelect
                    values={[currentValue]}
                    onUpdate={handleTypeChange}
                    isDisabled={!canEdit}
                    options={predefinedTypes}
                    size="sm"
                    width="fit-content"
                    showClear={false}
                />
            </TypeSelectWrapper>
        );
    };

    return (
        <BaseProperty {...props} values={currentType ? [currentType] : []} renderValue={renderValue} maxValues={1} />
    );
}
