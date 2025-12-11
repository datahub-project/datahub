/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { StructuredPropertyEntity } from '@types';

export const getPropertyDisplayName = (property: StructuredPropertyEntity) => {
    return property.definition?.displayName || property.definition?.qualifiedName || property.urn;
};

export const createPropertyUrnMap = (properties: StructuredPropertyEntity[]) => {
    const results = new Map();
    properties.forEach((property) => {
        results.set(property.urn, property);
    });
    return results;
};
