import { colors, Icon, Text } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';
import { MenuItem } from '@src/app/govern/structuredProperties/styledComponents';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import { useRemoveStructuredPropertiesMutation } from '@src/graphql/structuredProperties.generated';
import { EntityType, StructuredPropertyEntity } from '@src/types.generated';
import { Dropdown } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useEntityContext, useEntityData, useMutationUrn } from '../../../EntityContext';
import EditStructuredPropertyModal from './EditStructuredPropertyModal';

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
}

export function EditColumn({ structuredProperty, associatedUrn, values, refetch, isAddMode }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const { refetch: entityRefetch } = useEntityContext();
    const { entityType } = useEntityData();

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
    if (values && values?.length > 0) {
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
            />
            <ConfirmationModal
                isOpen={showConfirmRemove}
                handleClose={handleRemoveClose}
                handleConfirm={() => handleRemoveProperty()}
                modalTitle="Confirm Remove Structured Property"
                modalText={`Are you sure you want to remove ${structuredProperty.definition.displayName} from this asset?`}
            />
        </>
    );
}
