import { useCallback, useMemo } from 'react';

import { StructuredProp, getEntityTypeUrn, valueTypes } from '@app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { EntityType, PropertyCardinality, StructuredPropertyEntity } from '@src/types.generated';

const SETTINGS_DEFAULT = {
    isHidden: false,
    showInSearchFilters: false,
    showAsAssetBadge: false,
    showInAssetSummary: false,
    hideInAssetSummaryWhenEmpty: false,
    showInColumnsTable: false,
};

type Props = {
    selectedProperty?: StructuredPropertyEntity;
    setFormValues: React.Dispatch<React.SetStateAction<StructuredProp | undefined>>;
    setCardinality: React.Dispatch<React.SetStateAction<PropertyCardinality>>;
    setSelectedValueType: React.Dispatch<React.SetStateAction<string>>;
};

export default function useStructuredProp({
    selectedProperty,
    setFormValues,
    setCardinality,
    setSelectedValueType,
}: Props) {
    const entityRegistry = useEntityRegistry();

    const getEntitiesListOptions = useCallback(
        (entitiesList: EntityType[]) =>
            entitiesList.map((type) => ({
                label: entityRegistry.getEntityName(type) || '',
                value: getEntityTypeUrn(entityRegistry, type),
            })),
        [entityRegistry],
    );

    const handleSelectChange = useCallback(
        (field: string | string[], values: string[]) => {
            setFormValues((prev) =>
                field.includes('typeQualifier')
                    ? { ...prev, typeQualifier: { allowedTypes: values } }
                    : { ...prev, [String(field)]: values },
            );
        },
        [setFormValues],
    );

    // Edits to an existing property can only widen its scope, so already-saved selections are kept
    // even if the user deselects them in the dropdown.
    const handleSelectUpdateChange = useCallback(
        (field: string | string[], values: string[]) => {
            let initialValues: string[] = [];

            if (field === 'entityTypes')
                initialValues = selectedProperty?.definition?.entityTypes?.map((type) => type.urn) || [];

            if (field === 'allowedPlatforms')
                initialValues = selectedProperty?.definition?.allowedPlatforms?.map((platform) => platform.urn) || [];

            if (field.includes('typeQualifier'))
                initialValues =
                    selectedProperty?.definition?.typeQualifier?.allowedTypes?.map((type) => type.urn) || [];

            handleSelectChange(field, [...initialValues, ...values.filter((value) => !initialValues.includes(value))]);
        },
        [handleSelectChange, selectedProperty],
    );

    // Handle change in the property type dropdown
    const handleTypeUpdate = useCallback(
        (value: string) => {
            const typeOption = valueTypes.find((type) => type.value === value);
            setSelectedValueType(value);
            setFormValues((prev) => ({ ...prev, valueType: value }));
            setCardinality(
                typeOption?.cardinality === PropertyCardinality.Multiple
                    ? PropertyCardinality.Multiple
                    : PropertyCardinality.Single,
            );
        },
        [setCardinality, setFormValues, setSelectedValueType],
    );

    const handleDisplaySettingChange = useCallback(
        (settingField: string, value: boolean) => {
            setFormValues((prev) => {
                // Hiding the property turns off every other display surface.
                if (settingField === 'isHidden' && value) {
                    return { ...prev, settings: { ...SETTINGS_DEFAULT, isHidden: true } };
                }

                // Automatically disable `hideInAssetSummaryWhenEmpty` on disabling of `showInAssetSummary`
                if (settingField === 'showInAssetSummary' && !value) {
                    return {
                        ...prev,
                        settings: {
                            ...(prev?.settings || SETTINGS_DEFAULT),
                            showInAssetSummary: false,
                            hideInAssetSummaryWhenEmpty: false,
                        },
                    };
                }

                return {
                    ...prev,
                    settings: { ...(prev?.settings || SETTINGS_DEFAULT), [settingField]: value },
                };
            });
        },
        [setFormValues],
    );

    const disabledEntityTypeValues = useMemo(() => {
        return selectedProperty?.definition?.entityTypes?.map((type) => type.urn);
    }, [selectedProperty]);

    const disabledAllowedPlatformValues = useMemo(() => {
        return selectedProperty?.definition?.allowedPlatforms?.map((platform) => platform.urn);
    }, [selectedProperty]);

    const disabledTypeQualifierValues = useMemo(() => {
        return selectedProperty?.definition?.typeQualifier?.allowedTypes?.map((type) => type.urn);
    }, [selectedProperty]);

    return {
        handleSelectChange,
        handleSelectUpdateChange,
        handleTypeUpdate,
        getEntitiesListOptions,
        disabledEntityTypeValues,
        disabledAllowedPlatformValues,
        disabledTypeQualifierValues,
        handleDisplaySettingChange,
    };
}

export type StructuredPropActions = ReturnType<typeof useStructuredProp>;
