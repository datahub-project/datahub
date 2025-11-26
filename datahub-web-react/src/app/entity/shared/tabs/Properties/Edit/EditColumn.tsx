import { Dropdown } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityContext, useEntityData, useMutationUrn } from '@app/entity/shared/EntityContext';
import EditStructuredPropertyModal from '@app/entity/shared/tabs/Properties/Edit/EditStructuredPropertyModal';
import { Icon, Text, colors } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';
import { MenuItem } from '@src/app/govern/structuredProperties/styledComponents';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { ToastType, showToastMessage } from '@src/app/sharedV2/toastMessageUtils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useRemoveStructuredPropertiesMutation } from '@src/graphql/structuredProperties.generated';
import { EntityType, Maybe, SchemaFieldEntity, StructuredPropertyEntity } from '@src/types.generated';

export const MoreOptionsContainer = styled.div`
    display: flex;
    gap: 12px;
    justify-content: end;

    div {
        background-color: ${colors.gray[1500]};
        border-radius: 20px;
        width: 24px;
        height: 24px;
        padding: 3px;
        color: ${colors.gray[1800]};
        :hover {
            cursor: pointer;
        }
    }
`;

interface Props {
    structuredProperty?: StructuredPropertyEntity;
    associatedUrn?: string;
    values?: (string | number | null)[];
    refetch?: () => void;
    isAddMode?: boolean;
    fieldEntity?: Maybe<SchemaFieldEntity>;
}

export function EditColumn({ structuredProperty, associatedUrn, values, refetch, isAddMode, fieldEntity }: Props) {
    const entityRegistry = useEntityRegistry();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const { refetch: entityRefetch } = useEntityContext();
    const { entityType, entityData } = useEntityData();

    const canProposeProperties =
        !!entityData?.parent?.privileges?.canProposeStructuredProperties ||
        !!entityData?.privileges?.canProposeStructuredProperties;
    const canEditProperties =
        !!entityData?.parent?.privileges?.canEditProperties || !!entityData?.privileges?.canEditProperties;

    const [removeStructuredProperty] = useRemoveStructuredPropertiesMutation();

    const [showConfirmRemove, setShowConfirmRemove] = useState<boolean>(false);
    const mutationUrn = useMutationUrn();

    if (!structuredProperty || structuredProperty?.definition?.immutable) {
        return null;
    }

    const handleRemoveProperty = () => {
        showToastMessage(ToastType.LOADING, 'Removing structured property', 1);
        removeStructuredProperty({
            variables: {
                input: {
                    assetUrn: associatedUrn || mutationUrn,
                    structuredPropertyUrns: [structuredProperty.urn],
                },
            },
        })
            .then(() => {
                analytics.event({
                    type: EventType.RemoveStructuredPropertyEvent,
                    propertyUrn: structuredProperty.urn,
                    propertyType: structuredProperty.definition.valueType.urn,
                    assetUrn: associatedUrn || mutationUrn,
                    assetType: associatedUrn?.includes('urn:li:schemaField') ? EntityType.SchemaField : entityType,
                });
                showToastMessage(ToastType.SUCCESS, 'Structured property removed successfully!', 3);
                if (refetch) {
                    refetch();
                } else {
                    entityRefetch();
                }
            })
            .catch(() => {
                showToastMessage(ToastType.ERROR, 'Failed to remove structured property', 3);
            });

        setShowConfirmRemove(false);
    };

    const handleRemoveClose = () => {
        setShowConfirmRemove(false);
    };

    const items = [
        {
            key: '0',
            label: (
                <MenuItem
                    onClick={() => {
                        setIsEditModalVisible(true);
                    }}
                >
                    {isAddMode ? 'Add' : 'Edit'}
                </MenuItem>
            ),
        },
    ];
    if (values && values?.length > 0 && canEditProperties) {
        items.push({
            key: '1',
            label: (
                <MenuItem
                    onClick={() => {
                        setShowConfirmRemove(true);
                    }}
                >
                    <Text color="red"> Remove </Text>
                </MenuItem>
            ),
        });
    }

    return (
        <>
            <MoreOptionsContainer>
                <Dropdown menu={{ items }} trigger={['click']}>
                    <Icon icon="MoreVert" size="md" data-testid="structured-prop-entity-more-icon" />
                </Dropdown>
            </MoreOptionsContainer>
            <EditStructuredPropertyModal
                isOpen={isEditModalVisible}
                structuredProperty={structuredProperty}
                associatedUrn={associatedUrn}
                values={values}
                closeModal={() => setIsEditModalVisible(false)}
                refetch={refetch}
                isAddMode={isAddMode}
                fieldEntity={fieldEntity}
                canEdit={canEditProperties}
                canPropose={canProposeProperties}
            />
            <ConfirmationModal
                isOpen={showConfirmRemove}
                handleClose={handleRemoveClose}
                handleConfirm={() => handleRemoveProperty()}
                modalTitle="Confirm Remove Structured Property"
                modalText={`Are you sure you want to remove ${entityRegistry.getDisplayName(
                    structuredProperty.type,
                    structuredProperty,
                )} from this asset?`}
            />
        </>
    );
}
