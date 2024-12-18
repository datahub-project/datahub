import { LoadingOutlined } from '@ant-design/icons';
import { useApolloClient } from '@apollo/client';
import { Button, Text } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import {
    useCreateStructuredPropertyMutation,
    useUpdateStructuredPropertyMutation,
} from '@src/graphql/structuredProperties.generated';
import {
    AllowedValue,
    PropertyCardinality,
    SearchAcrossEntitiesInput,
    SearchResult,
    SearchResults,
    StructuredPropertyEntity,
    UpdateStructuredPropertyInput,
} from '@src/types.generated';
import { Form } from 'antd';
import { Tooltip } from '@components';
import React, { useEffect, useState } from 'react';
import AllowedValuesDrawer from './AllowedValuesDrawer';
import { updatePropertiesList } from './cacheUtils';
import StructuredPropsForm from './StructuredPropsForm';
import {
    DrawerHeader,
    FooterContainer,
    StyledDrawer,
    StyledIcon,
    StyledSpin,
    TitleContainer,
} from './styledComponents';
import useStructuredProp from './useStructuredProp';
import {
    getDisplayName,
    getNewAllowedTypes,
    getNewAllowedValues,
    getNewEntityTypes,
    getStringOrNumberValueField,
    getValueType,
    PropValueField,
    StructuredProp,
    valueTypes,
} from './utils';

interface Props {
    isDrawerOpen: boolean;
    setIsDrawerOpen: React.Dispatch<React.SetStateAction<boolean>>;
    selectedProperty?: SearchResult;
    setSelectedProperty: React.Dispatch<React.SetStateAction<SearchResult | undefined>>;
    refetch: () => void;
    inputs: SearchAcrossEntitiesInput;
    searchAcrossEntities?: SearchResults | null;
    badgeProperty?: StructuredPropertyEntity;
}

const StructuredPropsDrawer = ({
    isDrawerOpen,
    setIsDrawerOpen,
    selectedProperty,
    setSelectedProperty,
    refetch,
    inputs,
    searchAcrossEntities,
    badgeProperty,
}: Props) => {
    const [form] = Form.useForm();
    const [valuesForm] = Form.useForm();
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
    const [valueField, setValueField] = useState<PropValueField>('stringValue');

    const [showAllowedValuesDrawer, setShowAllowedValuesDrawer] = useState<boolean>(false);

    const { handleTypeUpdate } = useStructuredProp({
        selectedProperty,
        form,
        setFormValues,
        setCardinality,
        setSelectedValueType,
    });

    const isEditMode = !!selectedProperty;

    const clearValues = () => {
        form.resetFields();
        setIsDrawerOpen(false);
        setSelectedProperty(undefined);
        setFormValues(undefined);
        setSelectedValueType('');
        setAllowedValues(undefined);
        setCardinality(PropertyCardinality.Single);
        setShowAllowedValuesDrawer(false);
        valuesForm.resetFields();
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
                    settings: {
                        isHidden: updateValues.settings?.isHidden ?? false,
                        showInSearchFilters: updateValues.settings?.showInSearchFilters ?? false,
                        showAsAssetBadge: updateValues.settings?.showAsAssetBadge ?? false,
                        showInAssetSummary: updateValues.settings?.showInAssetSummary ?? false,
                        showInColumnsTable: updateValues.settings?.showInColumnsTable ?? false,
                    },
                };

                setIsLoading(true);
                updateStructuredProperty({
                    variables: {
                        input: editInput,
                    },
                })
                    .then(() => {
                        analytics.event({
                            type: EventType.EditStructuredPropertyEvent,
                            propertyUrn: selectedProperty.entity.urn,
                            propertyType:
                                valueTypes.find((valType) => valType.value === form.getFieldValue('valueType'))?.urn ||
                                '',
                            appliesTo: form.getFieldValue('entityTypes'),
                            qualifiedName: form.getFieldValue('qualifiedName'),
                            allowedAssetTypes: form.getFieldValue(['typeQualifier', 'allowedTypes']),
                            allowedValues: form.getFieldValue('allowedValues'),
                            cardinality,
                            isHidden: form.getFieldValue(['settings', 'isHidden']) ?? false,
                            showInSearchFilters: form.getFieldValue(['settings', 'showInSearchFilters']) ?? false,
                            showAsAssetBadge: form.getFieldValue(['settings', 'showAsAssetBadge']) ?? false,
                            showInAssetSummary: form.getFieldValue(['settings', 'showInAssetSummary']) ?? false,
                            showInColumnsTable: form.getFieldValue(['settings', 'showInColumnsTable']) ?? false,
                        });
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
                    qualifiedName: form.getFieldValue('qualifiedName') || undefined,
                    valueType: valueTypes.find((type) => type.value === form.getFieldValue('valueType'))?.urn,
                    allowedValues,
                    cardinality,
                    settings: {
                        isHidden: form.getFieldValue(['settings', 'isHidden']) ?? false,
                        showInSearchFilters: form.getFieldValue(['settings', 'showInSearchFilters']) ?? false,
                        showAsAssetBadge: form.getFieldValue(['settings', 'showAsAssetBadge']) ?? false,
                        showInAssetSummary: form.getFieldValue(['settings', 'showInAssetSummary']) ?? false,
                        showInColumnsTable: form.getFieldValue(['settings', 'showInColumnsTable']) ?? false,
                    },
                };

                setIsLoading(true);
                createStructuredProperty({
                    variables: {
                        input: createInput,
                    },
                })
                    .then((res) => {
                        analytics.event({
                            type: EventType.CreateStructuredPropertyEvent,
                            propertyType:
                                valueTypes.find((valType) => valType.value === form.getFieldValue('valueType'))?.urn ||
                                '',
                            appliesTo: form.getFieldValue('entityTypes'),
                            qualifiedName: form.getFieldValue('qualifiedName'),
                            allowedAssetTypes: form.getFieldValue(['typeQualifier', 'allowedTypes']),
                            allowedValues: form.getFieldValue('allowedValues'),
                            cardinality,
                            isHidden: form.getFieldValue(['settings', 'isHidden']) ?? false,
                            showInSearchFilters: form.getFieldValue(['settings', 'showInSearchFilters']) ?? false,
                            showAsAssetBadge: form.getFieldValue(['settings', 'showAsAssetBadge']) ?? false,
                            showInAssetSummary: form.getFieldValue(['settings', 'showInAssetSummary']) ?? false,
                            showInColumnsTable: form.getFieldValue(['settings', 'showInColumnsTable']) ?? false,
                        });

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

    useEffect(() => {
        if (selectedProperty) {
            const entity = selectedProperty.entity as StructuredPropertyEntity;
            const typeValue = getValueType(
                entity.definition.valueType.urn,
                entity.definition.cardinality || PropertyCardinality.Single,
            );

            const values: StructuredProp = {
                displayName: getDisplayName(entity),
                description: entity.definition.description,
                qualifiedName: entity.definition.qualifiedName,
                valueType: typeValue,
                entityTypes: entity.definition.entityTypes.map((entityType) => entityType.urn),
                typeQualifier: {
                    allowedTypes: entity.definition.typeQualifier?.allowedTypes?.map((entityType) => entityType.urn),
                },
                immutable: entity.definition.immutable,
                settings: entity.settings,
            };

            setFormValues(values);
            if (typeValue) handleTypeUpdate(typeValue);
            form.setFieldsValue(values);
        } else {
            setFormValues(undefined);
            form.resetFields();
            setSelectedValueType('');
        }

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedProperty, form]);

    useEffect(() => {
        const entity = selectedProperty?.entity as StructuredPropertyEntity;
        const field = getStringOrNumberValueField(selectedValueType);
        setValueField(field);
        const allowedList = entity?.definition?.allowedValues?.map((item) => {
            return {
                [field]: item.value[field],
                description: item.description,
            } as AllowedValue;
        });
        setAllowedValues(allowedList);
    }, [selectedProperty, selectedValueType, setAllowedValues]);

    const handleUpdateAllowedValues = () => {
        valuesForm.validateFields().then(() => {
            setAllowedValues(valuesForm.getFieldValue('allowedValues'));
            form.setFieldValue('allowedValues', valuesForm.getFieldValue('allowedValues'));
            setShowAllowedValuesDrawer(false);
        });
    };

    return (
        <StyledDrawer
            open={isDrawerOpen}
            closable={false}
            width={480}
            title={
                <DrawerHeader>
                    {showAllowedValuesDrawer ? (
                        <TitleContainer>
                            <StyledIcon
                                icon="ArrowBack"
                                color="gray"
                                size="3xl"
                                onClick={() => setShowAllowedValuesDrawer(false)}
                            />
                            <Text color="gray" weight="bold" size="lg">
                                Allowed Values
                            </Text>
                        </TitleContainer>
                    ) : (
                        <Text color="gray" weight="bold" size="lg">
                            {`${isEditMode ? 'Edit' : 'Create'} Structured Property`}
                        </Text>
                    )}
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
                        {showAllowedValuesDrawer ? (
                            <Button
                                style={{ display: 'block', width: '100%' }}
                                onClick={handleUpdateAllowedValues}
                                isDisabled={!canEditProps}
                            >
                                Update Allowed Values
                            </Button>
                        ) : (
                            <Button
                                style={{ display: 'block', width: '100%' }}
                                onClick={handleSubmit}
                                isDisabled={isLoading || !canEditProps}
                                data-testid="structured-props-create-update-button"
                            >
                                {isEditMode ? 'Update' : 'Create'}
                            </Button>
                        )}
                    </FooterContainer>
                </Tooltip>
            }
            destroyOnClose
        >
            <StyledSpin spinning={isLoading} indicator={<LoadingOutlined />}>
                {showAllowedValuesDrawer ? (
                    <>
                        <AllowedValuesDrawer
                            showAllowedValuesDrawer={showAllowedValuesDrawer}
                            propType={valueField}
                            allowedValues={allowedValues}
                            isEditMode={isEditMode}
                            noOfExistingValues={
                                (selectedProperty?.entity as StructuredPropertyEntity)?.definition?.allowedValues
                                    ?.length || 0
                            }
                            form={valuesForm}
                        />
                    </>
                ) : (
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
                        valueField={valueField}
                        setShowAllowedValuesDrawer={setShowAllowedValuesDrawer}
                        refetchProperties={refetch}
                        badgeProperty={badgeProperty}
                    />
                )}
            </StyledSpin>
        </StyledDrawer>
    );
};

export default StructuredPropsDrawer;
