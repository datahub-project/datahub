import analytics, { EventType } from '@src/app/analytics';
import { getFieldPathFromSchemaFieldUrn, getSourceUrnFromSchemaFieldUrn } from '@src/app/entityV2/schemaField/utils';
import { message } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import ProposalDescriptionModal from '@src/app/entityV2/shared/containers/profile/sidebar/ProposalDescriptionModal';
import { Modal } from '@src/alchemy-components';
import { useAppConfig } from '@src/app/useAppConfig';
import {
    // Saas-only mutation
    useProposeStructuredPropetiesMutation,
    useUpsertStructuredPropertiesMutation,
} from '../../../../../../graphql/structuredProperties.generated';
import {
    EntityType,
    Maybe,
    PropertyValueInput,
    SchemaFieldEntity,
    StructuredPropertyEntity,
    SubResourceType,
} from '../../../../../../types.generated';
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
    fieldEntity?: Maybe<SchemaFieldEntity>;
}

export default function EditStructuredPropertyModal({
    isOpen,
    structuredProperty,
    associatedUrn,
    values,
    closeModal,
    refetch,
    isAddMode,
    fieldEntity,
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
    const [showProposeModal, setShowProposeModal] = useState(false);
    const { config } = useAppConfig();
    const { showTaskCenterRedesign } = config.featureFlags;

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
                setShowProposeModal(false);
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
    function proposeProperties(description?: string) {
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
                    description,
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
                setTimeout(() => {
                    entityRefetch();
                    if (refetch) {
                        refetch();
                    }
                }, 3000);
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

    const handlePropose = () => {
        if (showTaskCenterRedesign) {
            setShowProposeModal(true);
        } else {
            proposeProperties();
        }
    };

    return (
        <>
            {!showProposeModal && (
                <Modal
                    title={`${isAddMode ? 'Add property' : 'Edit property'} ${entityRegistry.getDisplayName(
                        structuredProperty.type,
                        structuredProperty,
                    )}`}
                    onCancel={closeModal}
                    open={isOpen}
                    width={650}
                    buttons={[
                        { text: 'Cancel', key: 'Cancel', variant: 'text', onClick: closeModal, type: 'button' },
                        {
                            text: 'Propose',
                            key: 'Propose',
                            variant: 'outline',
                            onClick: handlePropose,
                            type: 'button',
                            disabled: !selectedValues.length,
                            buttonDataTestId: 'propose-update-structured-prop-on-entity-button',
                        },
                        {
                            text: isAddMode ? 'Add' : 'Update',
                            key: isAddMode ? 'Add' : 'Update',
                            variant: 'filled',
                            onClick: upsertProperties,
                            disabled: !selectedValues.length,
                            buttonDataTestId: 'add-update-structured-prop-on-entity-button',
                        },
                    ]}
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
                        fieldEntity={fieldEntity}
                    />
                </Modal>
            )}
            {showProposeModal && (
                <ProposalDescriptionModal onPropose={proposeProperties} onCancel={() => setShowProposeModal(false)} />
            )}
        </>
    );
}
