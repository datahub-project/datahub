import React, { useMemo } from 'react';
import styled from 'styled-components';

import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';
import AddPropertyButton from '@app/entityV2/summary/properties/menuAddProperty/AddPropertyButton';
import PropertyRenderer from '@app/entityV2/summary/properties/property/PropertyRenderer';

const Container = styled.div`
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    align-items: center;
`;

export default function Properties() {
    const { properties, editable } = useAssetPropertiesContext();

    const propertyItems = useMemo(
        () =>
            properties?.map((property, index) => ({
                property,
                key: `${property.type}-${index}`,
                index,
            })) ?? [],
        [properties],
    );

    return (
        <Container>
            {propertyItems.map((propertyItem) => (
                <PropertyRenderer
                    property={propertyItem.property}
                    position={propertyItem.index}
                    key={propertyItem.key}
                />
            ))}
            {editable && <AddPropertyButton />}
        </Container>
    );
}
