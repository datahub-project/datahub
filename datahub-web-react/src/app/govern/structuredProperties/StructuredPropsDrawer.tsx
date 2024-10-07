import { LoadingOutlined } from '@ant-design/icons';
import { useApolloClient } from '@apollo/client';
import { Button, Text } from '@src/alchemy-components';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import {
    useCreateStructuredPropertyMutation,
    useUpdateStructuredPropertyMutation,
} from '@src/graphql/structuredProperties.generated';
import {
    AllowedValue,
    PropertyCardinality,
    SearchResult,
    StructuredPropertyEntity,
    UpdateStructuredPropertyInput,
} from '@src/types.generated';
import { Form, Tooltip } from 'antd';
import React, { useState } from 'react';
import { useUserContext } from '@src/app/context/useUserContext';
import { updatePropertiesList } from './cacheUtils';
import StructuredPropsForm from './StructuredPropsForm';
import { DrawerHeader, FooterContainer, StyledDrawer, StyledIcon, StyledSpin } from './styledComponents';
import { getNewAllowedTypes, getNewAllowedValues, getNewEntityTypes, StructuredProp } from './utils';

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
    const me = useUserContext();
    const canEditProps = me.platformPrivileges?.manageStructuredProperties;

    const [createStructuredProperty] = useCreateStructuredPropertyMutation();
    const [updateStructuredProperty] = useUpdateStructuredPropertyMutation();
    const client = useApolloClient();

    const [cardinality, setCardinality] = useState<PropertyCardinality>(PropertyCardinality.Single);
    const [formValues, setFormValues] = useState<StructuredProp>();
    const [selectedValueType, setSelectedValueType] = useState<string>('');
    const [allowedValues, setAllowedValues] = useState<AllowedValue[] | undefined>([]);
    const [isLoading, setIsLoading] = useState<boolean>(false);

    const isEditMode = !!selectedProperty;

    const clearValues = () => {
        form.resetFields();
        setIsDrawerOpen(false);
        setSelectedProperty(undefined);
        setFormValues(undefined);
        setSelectedValueType('');
        setAllowedValues(undefined);
    };

    const handleClose = () => {
        clearValues();
    };

    const showErrorMessage = () => {
        showToastMessage(ToastType.ERROR, `Failed to ${isEditMode ? 'update' : 'create'} structured property.`, 3);
    };

    const showSuccessMessage = () => {
        showToastMessage(ToastType.SUCCESS, `Structured property ${isEditMode ? 'updated' : 'created'}!`, 3);
    };

    const handleSubmit = () => {
        if (isEditMode) {
            form.validateFields().then(() => {
                const updateValues = {
                    ...form.getFieldsValue(),
                    allowedValues,
                };

                const editInput: UpdateStructuredPropertyInput = {
                    urn: selectedProperty.entity.urn,
                    displayName: updateValues.displayName,
                    description: updateValues.description,
                    typeQualifier: {
                        newAllowedTypes: getNewAllowedTypes(
                            selectedProperty.entity as StructuredPropertyEntity,
                            updateValues,
                        ),
                    },
                    newEntityTypes: getNewEntityTypes(
                        selectedProperty.entity as StructuredPropertyEntity,
                        updateValues,
                    ),
                    newAllowedValues: getNewAllowedValues(
                        selectedProperty.entity as StructuredPropertyEntity,
                        updateValues,
                    ),
                    setCardinalityAsMultiple: cardinality === PropertyCardinality.Multiple,
                    filterStatus: updateValues.filterStatus,
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
                        clearValues();
                    });
            });
        } else {
            form.validateFields().then(() => {
                const createInput = {
                    ...form.getFieldsValue(),
                    allowedValues,
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
                        clearValues();
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
                <Tooltip
                    showArrow={false}
                    title={
                        !canEditProps
                            ? 'Must have permission to manage structured properties. Ask your DataHub administrator.'
                            : null
                    }
                >
                    <FooterContainer>
                        <Button
                            style={{ display: 'block', width: '100%' }}
                            onClick={handleSubmit}
                            isDisabled={isLoading || !canEditProps}
                        >
                            {isEditMode ? 'Update' : 'Create'}
                        </Button>
                    </FooterContainer>
                </Tooltip>
            }
            destroyOnClose
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
                    allowedValues={allowedValues}
                    setAllowedValues={setAllowedValues}
                />
            </StyledSpin>
        </StyledDrawer>
    );
};

export default StructuredPropsDrawer;
