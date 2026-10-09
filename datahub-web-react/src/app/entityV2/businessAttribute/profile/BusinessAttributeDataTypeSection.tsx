import { SimpleSelect, toast } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SchemaFieldDataType } from '@app/businessAttribute/businessAttributeUtils';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { useEntityData, useRefetch } from '@src/app/entity/shared/EntityContext';

import { useUpdateBusinessAttributeMutation } from '@graphql/businessAttribute.generated';

interface Props {
    readOnly?: boolean;
}

const SelectWrapper = styled.div`
    margin-top: 8px;
    width: 100%;
    overflow: hidden;
`;

// Ensures that any newly added datatype is automatically included in the user dropdown.
const DATA_TYPES = Object.values(SchemaFieldDataType);
export const BusinessAttributeDataTypeSection = ({ readOnly }: Props) => {
    const { t } = useTranslation('entity.types');
    const { urn, entityData } = useEntityData();
    const [originalDescription, setOriginalDescription] = useState<string | null>(null);
    const [isEditing, setEditing] = useState(false);
    const refetch = useRefetch();

    useEffect(() => {
        if (entityData?.properties?.businessAttributeDataType) {
            setOriginalDescription(entityData?.properties?.businessAttributeDataType);
        }
    }, [entityData]);

    const [updateBusinessAttribute] = useUpdateBusinessAttributeMutation();

    const handleChange = (values: string[]) => {
        const value = values[0];
        if (!value || value === originalDescription) {
            setEditing(false);
            return;
        }

        updateBusinessAttribute({ variables: { urn, input: { type: value } } })
            .then(() => {
                setEditing(false);
                setOriginalDescription(value);
                toast.success(t('businessAttribute.dataTypeUpdated'), { duration: 2 });
                refetch();
            })
            .catch((e: unknown) => {
                toast.destroy();
                if (e instanceof Error) {
                    toast.error(t('businessAttribute.dataTypeUpdateError', { error: e.message || '' }), {
                        duration: 3,
                    });
                }
            });
    };

    // Toggle editing mode
    const handleEditClick = () => {
        setEditing(!isEditing);
    };

    return (
        <SidebarSection
            title={t('businessAttribute.dataTypeLabel')}
            content={
                <>
                    {originalDescription}
                    {isEditing && (
                        <SelectWrapper>
                            <SimpleSelect
                                dataTestId="add-data-type-option"
                                placeholder={t('businessAttribute.dataTypePlaceholder')}
                                width="full"
                                showClear={false}
                                values={originalDescription ? [originalDescription] : []}
                                onUpdate={handleChange}
                                options={DATA_TYPES.map((dataType: SchemaFieldDataType) => ({
                                    value: dataType,
                                    label: dataType,
                                }))}
                            />
                        </SelectWrapper>
                    )}
                </>
            }
            extra={
                !readOnly && (
                    <SectionActionButton
                        icon={PencilSimple}
                        dataTestId="edit-data-type-button"
                        onClick={handleEditClick}
                    />
                )
            }
        />
    );
};
