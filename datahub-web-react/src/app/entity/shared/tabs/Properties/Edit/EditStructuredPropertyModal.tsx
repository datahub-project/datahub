import analytics, { EventType } from '@src/app/analytics';
import { Button, Modal, message } from 'antd';
import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components';
import { useUpsertStructuredPropertiesMutation } from '../../../../../../graphql/structuredProperties.generated';
import { EntityType, PropertyValueInput, StructuredPropertyEntity } from '../../../../../../types.generated';
import handleGraphQLError from '../../../../../shared/handleGraphQLError';
import StructuredPropertyInput from '../../../components/styled/StructuredProperty/StructuredPropertyInput';
import { useEditStructuredProperty } from '../../../components/styled/StructuredProperty/useEditStructuredProperty';
import { useEntityContext, useEntityData, useMutationUrn } from '../../../EntityContext';

const Description = styled.div`
    font-size: 14px;
    margin-bottom: 16px;
    margin-top: -8px;
`;

interface Props {
    isOpen: boolean;
    structuredProperty: StructuredPropertyEntity;
    associatedUrn?: string;
    values?: (string | number | null)[];
    closeModal: () => void;
    refetch?: () => void;
    isAddMode?: boolean;
}

export default function EditStructuredPropertyModal({
    isOpen,
    structuredProperty,
    associatedUrn,
    values,
    closeModal,
    refetch,
    isAddMode,
}: Props) {
    const { refetch: entityRefetch } = useEntityContext();
    const mutationUrn = useMutationUrn();
    const { entityType } = useEntityData();
    const urn = associatedUrn || mutationUrn;
    const initialValues = useMemo(() => values || [], [values]);
    const { selectedValues, selectSingleValue, toggleSelectedValue, updateSelectedValues, setSelectedValues } =
        useEditStructuredProperty(initialValues);
    const [upsertStructuredProperties] = useUpsertStructuredPropertiesMutation();

    useEffect(() => {
        setSelectedValues(initialValues);
    }, [isOpen, initialValues, setSelectedValues]);

    function upsertProperties() {
        message.loading(isAddMode ? 'Adding...' : 'Updating...');
        const propValues = selectedValues.map((value) => {
            if (typeof value === 'string') {
                return { stringValue: value as string };
            }
            return { numberValue: value as number };
        }) as PropertyValueInput[];
        upsertStructuredProperties({
            variables: {
                input: {
                    assetUrn: urn,
                    structuredPropertyInputParams: [
                        {
                            structuredPropertyUrn: structuredProperty.urn,
                            values: propValues,
                        },
                    ],
                },
            },
        })
            .then(() => {
                analytics.event({
                    type: isAddMode
                        ? EventType.ApplyStructuredPropertyEvent
                        : EventType.UpdateStructuredPropertyOnAssetEvent,
                    propertyUrn: structuredProperty.urn,
                    propertyType: structuredProperty.definition.valueType.urn,
                    assetUrn: urn,
                    assetType: associatedUrn?.includes('urn:li:schemaField') ? EntityType.SchemaField : entityType,
                    values: propValues,
                });
                if (refetch) {
                    refetch();
                } else {
                    entityRefetch();
                }
                message.destroy();
                message.success(`Successfully ${isAddMode ? 'added' : 'updated'} structured property!`);
                closeModal();
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: 'Unable to save structured property. Something went wrong.',
                });
                closeModal();
            });
    }

    return (
        <Modal
            title={`${isAddMode ? 'Add property' : 'Edit property'} ${structuredProperty?.definition?.displayName}`}
            onCancel={closeModal}
            open={isOpen}
            width={650}
            footer={
                <>
                    <Button onClick={closeModal} type="text">
                        Cancel
                    </Button>
                    <Button
                        type="primary"
                        onClick={upsertProperties}
                        disabled={!selectedValues.length}
                        data-testid="add-update-structured-prop-on-entity-button"
                    >
                        {isAddMode ? 'Add' : 'Update'}
                    </Button>
                </>
            }
            destroyOnClose
        >
            {structuredProperty?.definition?.description && (
                <Description>{structuredProperty.definition.description}</Description>
            )}
            <StructuredPropertyInput
                structuredProperty={structuredProperty}
                selectedValues={selectedValues}
                selectSingleValue={selectSingleValue}
                toggleSelectedValue={toggleSelectedValue}
                updateSelectedValues={updateSelectedValues}
            />
        </Modal>
    );
}
