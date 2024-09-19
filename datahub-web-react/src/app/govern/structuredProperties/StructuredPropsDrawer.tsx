import { LoadingOutlined } from '@ant-design/icons';
import { useApolloClient } from '@apollo/client';
import { Button, Text } from '@src/alchemy-components';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import {
    useCreateStructuredPropertyMutation,
    useUpdateStructuredPropertyMutation,
} from '@src/graphql/structuredProperties.generated';
import {
    PropertyCardinality,
    SearchResult,
    StructuredPropertyEntity,
    UpdateStructuredPropertyInput,
} from '@src/types.generated';
import { Form } from 'antd';
import React, { useState } from 'react';
import { updatePropertiesList } from './cacheUtils';
import StructuredPropsForm from './StructuredPropsForm';
import { DrawerHeader, FooterContainer, StyledDrawer, StyledIcon, StyledSpin } from './styledComponents';
import { getNewAllowedTypes, getNewEntityTypes, StructuredProp } from './utils';

interface Props {
    isDrawerOpen: boolean;
    setIsDrawerOpen: React.Dispatch<React.SetStateAction<boolean>>;
    selectedProperty?: SearchResult;
    setSelectedProperty: React.Dispatch<React.SetStateAction<SearchResult | undefined>>;
    refetch: () => void;
    inputs: object;
    searchAcrossEntities?: object | null;
}

const StructuredPropsDrawer = ({
    isDrawerOpen,
    setIsDrawerOpen,
    selectedProperty,
    setSelectedProperty,
    refetch,
    inputs,
    searchAcrossEntities,
}: Props) => {
    const [form] = Form.useForm();

    const [createStructuredProperty] = useCreateStructuredPropertyMutation();
    const [updateStructuredProperty] = useUpdateStructuredPropertyMutation();
    const client = useApolloClient();

    const [cardinality, setCardinality] = useState<PropertyCardinality>(PropertyCardinality.Single);
    const [formValues, setFormValues] = useState<StructuredProp>();
    const [selectedValueType, setSelectedValueType] = useState<string>('');
    const [isLoading, setIsLoading] = useState<boolean>(false);

    const isEditMode = !!selectedProperty;

    const handleClose = () => {
        setIsDrawerOpen(false);
        setSelectedProperty(undefined);
        form.resetFields();
        setFormValues(undefined);
        setSelectedValueType('');
    };

    const showErrorMessage = () => {
        showToastMessage(ToastType.ERROR, `Failed to ${isEditMode ? 'update' : 'create'} structured property.`, 3);
    };

    const showSuccessMessage = () => {
        showToastMessage(ToastType.SUCCESS, `Structured property ${isEditMode ? 'updated' : 'created'}!`, 3);
    };

    const handleSubmit = () => {
        const formData = form.getFieldsValue();

        if (isEditMode) {
            form.validateFields().then(() => {
                const values: StructuredProp = form.getFieldsValue();

                const editInput: UpdateStructuredPropertyInput = {
                    urn: selectedProperty.entity.urn,
                    displayName: values.displayName,
                    description: values.description,
                    typeQualifier: {
                        newAllowedTypes: getNewAllowedTypes(
                            selectedProperty.entity as StructuredPropertyEntity,
                            values,
                        ),
                    },
                    newEntityTypes: getNewEntityTypes(selectedProperty.entity as StructuredPropertyEntity, values),
                    setCardinalityAsMultiple: cardinality === PropertyCardinality.Multiple,
                };
                setIsLoading(true);
                updateStructuredProperty({
                    variables: {
                        input: editInput,
                    },
                })
                    .then(() => {
                        refetch();
                        showSuccessMessage();
                    })
                    .catch(() => {
                        showErrorMessage();
                    })
                    .finally(() => {
                        setIsLoading(false);
                        form.resetFields();
                        setIsDrawerOpen(false);
                        setSelectedProperty(undefined);
                        setFormValues(undefined);
                        setSelectedValueType('');
                    });
            });
        } else {
            // Add default qualified name based on the displayName
            if (!formData.qualifiedName)
                form.setFieldValue('qualifiedName', formData.displayName?.replace(/\s/g, '').toLowerCase());

            form.validateFields().then(() => {
                const createInput = {
                    ...form.getFieldsValue(),
                    cardinality,
                };
                setIsLoading(true);
                createStructuredProperty({
                    variables: {
                        input: createInput,
                    },
                })
                    .then((res) => {
                        showSuccessMessage();
                        updatePropertiesList(client, inputs, res.data?.createStructuredProperty, searchAcrossEntities);
                    })
                    .catch(() => {
                        showErrorMessage();
                    })
                    .finally(() => {
                        setIsLoading(false);
                        form.resetFields();
                        setIsDrawerOpen(false);
                        setSelectedProperty(undefined);
                        setFormValues(undefined);
                        setSelectedValueType('');
                    });
            });
        }
    };

    return (
        <StyledDrawer
            open={isDrawerOpen}
            closable={false}
            width={480}
            title={
                <DrawerHeader>
                    <Text color="gray" weight="bold">
                        {`${isEditMode ? 'Edit' : 'Create'} Structured Property`}
                    </Text>
                    <StyledIcon icon="Close" color="gray" onClick={handleClose} />
                </DrawerHeader>
            }
            footer={
                <FooterContainer>
                    <Button style={{ display: 'block', width: '100%' }} onClick={handleSubmit} isDisabled={isLoading}>
                        {isEditMode ? 'Update' : 'Create'}
                    </Button>
                </FooterContainer>
            }
        >
            <StyledSpin spinning={isLoading} indicator={<LoadingOutlined />}>
                <StructuredPropsForm
                    selectedProperty={selectedProperty}
                    form={form}
                    formValues={formValues}
                    setFormValues={setFormValues}
                    setCardinality={setCardinality}
                    isEditMode={isEditMode}
                    selectedValueType={selectedValueType}
                    setSelectedValueType={setSelectedValueType}
                />
            </StyledSpin>
        </StyledDrawer>
    );
};

export default StructuredPropsDrawer;
