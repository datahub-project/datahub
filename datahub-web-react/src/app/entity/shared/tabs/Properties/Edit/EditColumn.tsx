import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ItemType } from '@components/components/Menu/types';

import { useEntityContext, useEntityData, useMutationUrn } from '@app/entity/shared/EntityContext';
import EditStructuredPropertyModal from '@app/entity/shared/tabs/Properties/Edit/EditStructuredPropertyModal';
import { Button, Menu } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { ToastType, showToastMessage } from '@src/app/sharedV2/toastMessageUtils';
import { useRemoveStructuredPropertiesMutation } from '@src/graphql/structuredProperties.generated';
import { EntityType, StructuredPropertyEntity } from '@src/types.generated';

const MoreOptionsContainer = styled.div`
    display: flex;
    gap: 12px;
    justify-content: end;
`;

interface Props {
    structuredProperty?: StructuredPropertyEntity;
    associatedUrn?: string;
    values?: (string | number | null)[];
    refetch?: () => void;
    isAddMode?: boolean;
}

export function EditColumn({ structuredProperty, associatedUrn, values, refetch, isAddMode }: Props) {
    const { t } = useTranslation('entity.profile.tabs');
    const { t: tc } = useTranslation(['common.actions', 'common.feedback']);
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
        showToastMessage(ToastType.LOADING, t('properties.removing.loading'), 1);
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
                showToastMessage(ToastType.SUCCESS, t('properties.removed.success'), 3);
                if (refetch) {
                    refetch();
                } else {
                    entityRefetch();
                }
            })
            .catch(() => {
                showToastMessage(ToastType.ERROR, t('properties.removed.error'), 3);
            });

        setShowConfirmRemove(false);
    };

    const handleRemoveClose = () => {
        setShowConfirmRemove(false);
    };

    const items: ItemType[] = [
        {
            type: 'item',
            key: '0',
            title: isAddMode ? tc('common.actions:add') : tc('common.actions:edit'),
            onClick: () => setIsEditModalVisible(true),
        },
    ];
    if (values && values?.length > 0) {
        items.push({
            type: 'item',
            key: '1',
            title: tc('common.actions:remove'),
            danger: true,
            onClick: () => setShowConfirmRemove(true),
        });
    }

    return (
        <>
            <MoreOptionsContainer>
                <Menu items={items}>
                    <Button
                        variant="text"
                        color="gray"
                        isCircle
                        icon={{ icon: DotsThreeVertical, size: 'xl', weight: 'bold' }}
                        data-testid="structured-prop-entity-more-icon"
                    />
                </Menu>
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
                modalTitle={t('properties.confirmRemove.title')}
                modalText={t('properties.confirmRemove.confirmation', {
                    name: structuredProperty.definition.displayName,
                })}
            />
        </>
    );
}
