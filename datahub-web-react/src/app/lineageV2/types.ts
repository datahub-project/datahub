import { FetchedEntity } from '../lineage/types';
import { Container, EntityType, SchemaFieldDataType } from '../../types.generated';

export enum LineageAssetType {
    Column,
    Entity,
    DataProduct,
}

interface LineageAssetBase {
    name: string;
}

export interface ColumnAsset extends LineageAssetBase {
    type: LineageAssetType.Column;
    dataType?: SchemaFieldDataType;
    nativeDataType?: string | null;
}

export interface EntityAsset extends LineageAssetBase {
    type: LineageAssetType.Entity;
    entityType: EntityType;
}

export interface DataProductAsset extends LineageAssetBase {
    type: LineageAssetType.DataProduct;
    size?: number;
}

export type LineageAsset = ColumnAsset | EntityAsset | DataProductAsset;

export interface FetchedEntityV2 extends FetchedEntity {
    lineageAssets?: LineageAsset[];
    parentContainers?: Container[];
}
