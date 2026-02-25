import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/document/hooks/useDocumentPermissions';
import { useUpdateDocument } from '@app/document/hooks/useUpdateDocument';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { SimpleSelect } from '@src/alchemy-components';

import { Document } from '@types';

const TypeSelectWrapper = styled.div`
    overflow: hidden;
`;

const NONE_VALUE = '';

const typeOptions = [
    { label: 'None', value: NONE_VALUE },
    { label: 'Runbook', value: 'Runbook' },
    { label: 'FAQ', value: 'FAQ' },
    { label: 'Insight', value: 'Insight' },
    { label: 'Definition', value: 'Definition' },
    { label: 'Decision', value: 'Decision' },
];

export default function DocumentTypeProperty(props: PropertyComponentProps) {
    const { urn, entityData } = useEntityData();
    const document = entityData as Document;
    const refetch = useRefetch();
    const { canEditType } = useDocumentPermissions(urn);
    const { updateSubType } = useUpdateDocument();

    const serverType = document?.subType?.trim() || NONE_VALUE;
    const [optimisticType, setOptimisticType] = useState(serverType);

    // Sync optimistic state with server state when it changes
    useEffect(() => {
        setOptimisticType(serverType);
    }, [serverType]);

    const handleTypeChange = async (values: string[]) => {
        const selectedValue = values[0] || NONE_VALUE;
        const previousType = optimisticType;

        // Optimistically update the UI immediately
        setOptimisticType(selectedValue);

        try {
            // Send empty string or null for "None", otherwise send the selected value
            const typeToSend = selectedValue === NONE_VALUE ? null : selectedValue;
            await updateSubType({
                urn,
                subType: typeToSend,
            });
            await refetch();
        } catch (error) {
            // Revert to previous type if the mutation fails
            console.error('[DocumentTypeProperty] Update failed, reverting to:', previousType);
            setOptimisticType(previousType);
        }
    };

    const renderValue = () => {
        if (!canEditType) {
            // Show read-only value
            const displayValue = optimisticType === NONE_VALUE ? 'None' : optimisticType;
            return <span>{displayValue}</span>;
        }

        // If the actual type is not in the options, add it to the options
        const isCustomType = optimisticType !== NONE_VALUE && !typeOptions.some((opt) => opt.value === optimisticType);
        const finalTypeOptions = isCustomType
            ? [...typeOptions, { label: optimisticType, value: optimisticType }]
            : typeOptions;

        return (
            <TypeSelectWrapper data-testid="document-type-select">
                <SimpleSelect
                    values={[optimisticType]}
                    onUpdate={handleTypeChange}
                    isDisabled={!canEditType}
                    options={finalTypeOptions}
                    size="sm"
                    width="fit-content"
                    showClear={false}
                />
            </TypeSelectWrapper>
        );
    };

    return <BaseProperty {...props} values={[optimisticType]} renderValue={renderValue} maxValues={1} />;
}
