import { Button, Modal, message } from 'antd';
import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import StructuredPropertyInput from '../../../components/styled/StructuredProperty/StructuredPropertyInput';
import { PropertyValueInput, StructuredPropertyEntity } from '../../../../../../types.generated';
import { useUpsertStructuredPropertiesMutation } from '../../../../../../graphql/structuredProperties.generated';
import { useEditStructuredProperty } from '../../../components/styled/StructuredProperty/useEditStructuredProperty';
import { useEntityContext, useMutationUrn } from '../../../EntityContext';
import handleGraphQLError from '../../../../../shared/handleGraphQLError';

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
}

export default function EditStructuredPropertyModal({
    isOpen,
    structuredProperty,
    associatedUrn,
    values,
    closeModal,
    refetch,
}: Props) {
    const { t } = useTranslation();
    const { refetch: entityRefetch } = useEntityContext();
    const mutationUrn = useMutationUrn();
    const urn = associatedUrn || mutationUrn;
    const initialValues = useMemo(() => values || [], [values]);
    const { selectedValues, selectSingleValue, toggleSelectedValue, updateSelectedValues, setSelectedValues } =
        useEditStructuredProperty(initialValues);
    const [upsertStructuredProperties] = useUpsertStructuredPropertiesMutation();

    useEffect(() => {
        setSelectedValues(initialValues);
    }, [isOpen, initialValues, setSelectedValues]);

    function upsertProperties() {
        message.loading(t('crud.updating'));
        upsertStructuredProperties({
            variables: {
                input: {
                    assetUrn: urn,
                    structuredPropertyInputParams: [
                        {
                            structuredPropertyUrn: structuredProperty.urn,
                            values: selectedValues.map((value) => {
                                if (typeof value === 'string') {
                                    return { stringValue: value as string };
                                }
                                return { numberValue: value as number };
                            }) as PropertyValueInput[],
                        },
                    ],
                },
            },
        })
            .then(() => {
                if (refetch) {
                    refetch();
                } else {
                    entityRefetch();
                }
                message.destroy();
                message.success(t('crud.success.successfulUpdated'));
                closeModal();
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: t('crud.error.unableToSaveStructured'),
                });
                closeModal();
            });
    }

    return (
        <Modal
            title={structuredProperty.definition.displayName}
            onCancel={closeModal}
            open={isOpen}
            width={650}
            footer={
                <>
                    <Button onClick={closeModal} type="text">
                        {t('common.cancel')}
                    </Button>
                    <Button type="primary" onClick={upsertProperties} disabled={!selectedValues.length}>
                        {t('common.update')}
                    </Button>
                </>
            }
            destroyOnClose
        >
            {structuredProperty?.definition.description && (
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
