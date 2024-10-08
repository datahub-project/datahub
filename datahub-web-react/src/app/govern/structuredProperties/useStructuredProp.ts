import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import {
    EntityType,
    PropertyCardinality,
    SearchResult,
    StructuredPropertyEntity,
    StructuredPropertyFilterStatus,
} from '@src/types.generated';
import { FormInstance } from 'antd';
import { useMemo } from 'react';
import { getEntityTypeUrn, StructuredProp, valueTypes } from './utils';

interface Props {
    selectedProperty?: SearchResult;
    form: FormInstance;
    setFormValues: React.Dispatch<React.SetStateAction<StructuredProp | undefined>>;
    setCardinality: React.Dispatch<React.SetStateAction<PropertyCardinality>>;
    setSelectedValueType: React.Dispatch<React.SetStateAction<string>>;
}

export default function useStructuredProp({
    selectedProperty,
    form,
    setFormValues,
    setCardinality,
    setSelectedValueType,
}: Props) {
    const entityRegistry = useEntityRegistryV2();

    const getEntitiesListOptions = (entitiesList: EntityType[]) => {
        const listOptions: { label: string; value: string }[] = [];
        entitiesList.forEach((type) => {
            const entity = {
                label: entityRegistry.getEntityName(type) || '',
                value: getEntityTypeUrn(entityRegistry, type),
            };
            listOptions.push(entity);
        });
        return listOptions;
    };

    const updateFormValues = (field, values) => {
        if (field.includes('typeQualifier')) {
            setFormValues((prev) => ({
                ...prev,
                typeQualifier: {
                    allowedTypes: values,
                },
            }));
        } else
            setFormValues((prev) => ({
                ...prev,
                [field]: values,
            }));
    };

    const handleSelectChange = (field, values) => {
        form.setFieldValue(field, values);
        updateFormValues(field, values);
    };

    const handleSelectUpdateChange = (field, values) => {
        const entity = selectedProperty?.entity as StructuredPropertyEntity;
        let initialValues: string[] = [];

        if (field === 'entityTypes') initialValues = entity.definition.entityTypes.map((type) => type.urn);

        if (field.includes('typeQualifier'))
            initialValues = entity.definition.typeQualifier?.allowedTypes?.map((type) => type.urn) || [];

        const updatedValues = [...initialValues, ...values.filter((value) => !initialValues.includes(value))];

        form.setFieldValue(field, updatedValues);
        updateFormValues(field, updatedValues);
    };

    // Handle change in the property type dropdown
    const handleTypeUpdate = (value: string) => {
        const typeOption = valueTypes.find((type) => type.value === value);
        setSelectedValueType(value);
        handleSelectChange('valueType', value);
        setFormValues((prev) => ({
            ...prev,
            valueType: value,
        }));

        const isList = typeOption?.cardinality === PropertyCardinality.Multiple;
        if (isList) setCardinality(PropertyCardinality.Multiple);
        else setCardinality(PropertyCardinality.Single);
    };

    const handleFilterStatusChange = (showInFilters: boolean) => {
        const filterStatus = showInFilters
            ? StructuredPropertyFilterStatus.Enabled
            : StructuredPropertyFilterStatus.Disabled;
        handleSelectChange('filterStatus', filterStatus);
        setFormValues((prev) => ({
            ...prev,
            filterStatus,
        }));
    };

    const disabledEntityTypeValues = useMemo(() => {
        return (selectedProperty?.entity as StructuredPropertyEntity)?.definition?.entityTypes?.map((type) => type.urn);
    }, [selectedProperty]);

    const disabledTypeQualifierValues = useMemo(() => {
        return (selectedProperty?.entity as StructuredPropertyEntity)?.definition?.typeQualifier?.allowedTypes?.map(
            (type) => type.urn,
        );
    }, [selectedProperty]);

    return {
        handleSelectChange,
        handleSelectUpdateChange,
        handleTypeUpdate,
        getEntitiesListOptions,
        disabledEntityTypeValues,
        disabledTypeQualifierValues,
        handleFilterStatusChange,
    };
}
