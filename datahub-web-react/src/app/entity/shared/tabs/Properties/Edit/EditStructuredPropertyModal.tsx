import analytics, { EventType } from '@src/app/analytics';
<<<<<<< ours
import { getFieldPathFromSchemaFieldUrn, getSourceUrnFromSchemaFieldUrn } from '@src/app/entityV2/schemaField/utils';
import { Button, Modal, message } from 'antd';
||||||| base
import { Button, Modal, message } from 'antd';
=======
import { Modal, message } from 'antd';
>>>>>>> theirs
import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components';
<<<<<<< ours
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import {
    // Saas-only mutation
    useProposeStructuredPropetiesMutation,
    useUpsertStructuredPropertiesMutation,
} from '../../../../../../graphql/structuredProperties.generated';
import {
    EntityType,
    PropertyValueInput,
    StructuredPropertyEntity,
    SubResourceType,
} from '../../../../../../types.generated';
||||||| base
import { useUpsertStructuredPropertiesMutation } from '../../../../../../graphql/structuredProperties.generated';
import { EntityType, PropertyValueInput, StructuredPropertyEntity } from '../../../../../../types.generated';
=======
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { useUpsertStructuredPropertiesMutation } from '../../../../../../graphql/structuredProperties.generated';
import { EntityType, PropertyValueInput, StructuredPropertyEntity } from '../../../../../../types.generated';
>>>>>>> theirs
import handleGraphQLError from '../../../../../shared/handleGraphQLError';
import { useEntityContext, useEntityData, useMutationUrn } from '../../../EntityContext';
import StructuredPropertyInput from '../../../components/styled/StructuredProperty/StructuredPropertyInput';
import { useEditStructuredProperty } from '../../../components/styled/StructuredProperty/useEditStructuredProperty';

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
    const entityRegistry = useEntityRegistryV2();
    const { refetch: entityRefetch } = useEntityContext();
    const mutationUrn = useMutationUrn();
    const { entityType } = useEntityData();
    const urn = associatedUrn || mutationUrn;
    const initialValues = useMemo(() => values || [], [values]);
    const { selectedValues, selectSingleValue, toggleSelectedValue, updateSelectedValues, setSelectedValues } =
        useEditStructuredProperty(initialValues);
    const [upsertStructuredProperties] = useUpsertStructuredPropertiesMutation();

    // is schema field urn
    const isSchemaField = urn.includes('urn:li:schemaField');

    const resource = isSchemaField ? getSourceUrnFromSchemaFieldUrn(urn) : urn;
    const subresource = isSchemaField ? getFieldPathFromSchemaFieldUrn(urn) : undefined;

    // Saas-only mutation
    const [proposeStructuredProperties] = useProposeStructuredPropetiesMutation();

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

    // Saas-only mutation
    function proposeProperties() {
        message.loading('Proposing...');

        const propValues = selectedValues.map((value) => {
            if (typeof value === 'string') {
                return { stringValue: value as string };
            }
            return { numberValue: value as number };
        }) as PropertyValueInput[];

        proposeStructuredProperties({
            variables: {
                input: {
                    resourceUrn: resource,
                    subResource: subresource,
                    subResourceType: isSchemaField ? SubResourceType.DatasetField : undefined,
                    structuredProperties: [
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
                    type: EventType.ProposeStructuredPropertiesMutation,
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
                message.success('Successfully proposed structured property. It is pending approval.');
                closeModal();
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: 'Unable to propose structured property. Something went wrong.',
                    badRequestMessage:
                        'Failed to propose structured property. Property with these values is already proposed or applied.',
                    permissionMessage:
                        'Unauthorized to propose property. The "Propose Structured Properties" privilege is required for this asset.',
                });
                closeModal();
            });
    }

    return (
        <Modal
            title={`${isAddMode ? 'Add property' : 'Edit property'} ${entityRegistry.getDisplayName(
                structuredProperty.type,
                structuredProperty,
            )}`}
            onCancel={closeModal}
            open={isOpen}
            width={650}
            footer={
                <ModalButtonContainer>
                    <Button variant="text" onClick={closeModal} color="gray">
                        Cancel
                    </Button>
                    <Button
<<<<<<< ours
                        type="default"
                        onClick={proposeProperties}
                        disabled={!selectedValues.length}
                        data-testid="propose-update-structured-prop-on-entity-button"
                    >
                        Propose
                    </Button>
                    <Button
                        type="primary"
||||||| base
                        type="primary"
=======
>>>>>>> theirs
                        onClick={upsertProperties}
                        disabled={!selectedValues.length}
                        data-testid="add-update-structured-prop-on-entity-button"
                    >
                        {isAddMode ? 'Add' : 'Update'}
                    </Button>
                </ModalButtonContainer>
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
