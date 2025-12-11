/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SummaryElementType } from '@types';

/**
 * Validates position input for operations that require position
 */
export const validatePosition = (position: number, operationName: string): string | null => {
    if (position < 0) {
        return `Position must be non-negative for ${operationName}`;
    }
    return null;
};

/**
 * Validates array bounds for position-based operations
 */
export const validateArrayBounds = (position: number, arrayLength: number, operationName: string): string | null => {
    if (position >= arrayLength || position < 0) {
        return `Position is out of bounds for ${operationName}`;
    }
    return null;
};

/**
 * Validates element type for summary operations
 */
export const validateElementType = (elementType: SummaryElementType): string | null => {
    if (!elementType) {
        return 'Element type is required';
    }
    return null;
};

/**
 * Validates structured property for STRUCTURED_PROPERTY element type
 */
export const validateStructuredProperty = (
    elementType: SummaryElementType,
    structuredPropertyUrn?: string,
): string | null => {
    if (elementType === SummaryElementType.StructuredProperty && !structuredPropertyUrn) {
        return 'Structured property URN is required for STRUCTURED_PROPERTY element type';
    }
    return null;
};
