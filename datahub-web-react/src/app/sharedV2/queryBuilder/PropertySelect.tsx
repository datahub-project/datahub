import { SelectOption, SimpleSelect } from '@components';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { ConditionElementWithFixedWidth } from '@app/sharedV2/queryBuilder/styledComponents';

interface Props {
    selectedProperty?: string;
    properties: Property[];
    onChangeProperty: (propertyId?: string) => void;
}

function toOptions(properties: Property[]): SelectOption[] {
    return properties.map((property) => ({
        value: property.id.toString(),
        label: property.displayName,
        description: property.description,
    }));
}

const PropertySelect = ({ selectedProperty, properties, onChangeProperty }: Props) => {
    const { t } = useTranslation('shared.query-builder');

    // A selected value can be a top-level property or a child of a parent group. When it's a
    // child, we surface its parent so the group stays selected while editing an existing filter.
    const parentOfSelected = useMemo(
        () => properties.find((property) => (property.children ?? []).some((child) => child.id === selectedProperty)),
        [properties, selectedProperty],
    );

    // Track the chosen group locally so the child picker stays open after a group is picked but
    // before a child is selected (at which point there is no leaf value in the predicate yet).
    const [openGroupId, setOpenGroupId] = useState<string | undefined>(undefined);

    // Condition rows are rendered from an unkeyed list, so a delete can rebind this instance to a
    // different predicate. Drop the transient group whenever the bound property changes to a
    // concrete leaf, so a stale group never leaks onto another condition.
    const [prevSelectedProperty, setPrevSelectedProperty] = useState(selectedProperty);
    if (selectedProperty !== prevSelectedProperty) {
        setPrevSelectedProperty(selectedProperty);
        if (selectedProperty) {
            setOpenGroupId(undefined);
        }
    }

    const activeGroupId = parentOfSelected?.id ?? (selectedProperty ? undefined : openGroupId);
    const activeGroup = useMemo(
        () => properties.find((property) => property.id === activeGroupId),
        [properties, activeGroupId],
    );

    const topLevelValue =
        activeGroupId ??
        (properties.some((property) => property.id === selectedProperty) ? selectedProperty : undefined);

    const handleTopLevelChange = (propertyId: string) => {
        const picked = properties.find((property) => property.id === propertyId);
        if (picked?.children?.length) {
            setOpenGroupId(propertyId);
            // Clear any previously selected leaf until a child in the new group is chosen.
            if (selectedProperty) {
                onChangeProperty(undefined);
            }
            return;
        }
        setOpenGroupId(undefined);
        onChangeProperty(propertyId);
    };

    return (
        <>
            <ConditionElementWithFixedWidth>
                <SimpleSelect
                    options={toOptions(properties)}
                    onUpdate={(val) => handleTopLevelChange(val[0])}
                    values={topLevelValue ? [topLevelValue] : []}
                    placeholder={t('property.placeholder')}
                    dataTestId="condition-select"
                    width="full"
                    showClear={false}
                />
            </ConditionElementWithFixedWidth>
            {activeGroup?.children?.length ? (
                <ConditionElementWithFixedWidth>
                    <SimpleSelect
                        options={toOptions(activeGroup.children)}
                        onUpdate={(val) => onChangeProperty(val[0])}
                        values={selectedProperty ? [selectedProperty] : []}
                        placeholder={t('property.placeholder')}
                        dataTestId="condition-select-child"
                        width="full"
                        showClear={false}
                    />
                </ConditionElementWithFixedWidth>
            ) : null}
        </>
    );
};

export default PropertySelect;
