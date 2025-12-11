/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DataTypeEntity, Entity, StructuredPropertyEntity } from '@types';

export interface ValueColumnData {
    value: string | number | null;
    entity: Entity | null;
}

export interface TypeData {
    type: string;
    nativeDataType: string;
}

export interface PropertyRow {
    displayName: string;
    qualifiedName: string;
    values?: ValueColumnData[];
    children?: PropertyRow[];
    childrenCount?: number;
    parent?: PropertyRow;
    depth?: number;
    type?: TypeData;
    dataType?: DataTypeEntity;
    isParentRow?: boolean;
    structuredProperty?: StructuredPropertyEntity;
    associatedUrn?: string;
}
