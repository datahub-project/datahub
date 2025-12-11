/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';
import styled from 'styled-components';

import AddPropertyButton from '@app/entityV2/summary/properties/menuAddProperty/AddPropertyButton';
import PropertyRenderer from '@app/entityV2/summary/properties/property/PropertyRenderer';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

const Container = styled.div`
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    align-items: center;
`;

export default function Properties() {
    const { summaryElements, isTemplateEditable } = usePageTemplateContext();

    const propertyItems = useMemo(
        () =>
            summaryElements?.map((property, index) => ({
                property,
                key: `${property.type}-${index}`,
                index,
            })) ?? [],
        [summaryElements],
    );

    return (
        <Container data-testid="properties-section">
            {propertyItems.map((propertyItem) => (
                <PropertyRenderer
                    property={propertyItem.property}
                    position={propertyItem.index}
                    key={propertyItem.key}
                />
            ))}
            {isTemplateEditable && <AddPropertyButton />}
        </Container>
    );
}
