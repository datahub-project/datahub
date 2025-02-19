import { QueryHookOptions, QueryResult } from '@apollo/client';
import { downgradeV2FieldPath } from '@app/lineageV2/lineageUtils';
import React from 'react';
import { EntityLineageV2Fragment, LineageSchemaFieldFragment } from '@graphql/lineage.generated';
import { Entity as EntityInterface, EntityType, Exact, FeatureFlagsConfig, SearchResult } from '../../types.generated';
import { GenericEntityProperties } from '../entity/shared/types';
import { FetchedEntity } from '../lineage/types';
import { FetchedEntityV2, FetchedEntityV2Relationship, LineageAsset, LineageAssetType } from '../lineageV2/types';
import { SearchResultProvider } from '../search/context/SearchResultContext';
import DefaultEntity from './DefaultEntity';
import { Entity, EntityCapabilityType, EntityMenuActions, IconStyleType, PreviewType } from './Entity';
import PreviewContext from './shared/PreviewContext';
import { GLOSSARY_ENTITY_TYPES } from './shared/constants';
import { EntitySidebarSection, EntitySidebarTab } from './shared/types';
import { dictToQueryStringParams, getFineGrainedLineageWithSiblings, urlEncodeUrn } from './shared/utils';

function validatedGet<K, V>(key: K, map: Map<K, V>, def: V): V {
    if (map.has(key)) {
        return map.get(key) as V;
    }
    return def;
}

/**
 * Serves as a singleton registry for all DataHub entities to appear on the frontend.
 */
export default class EntityRegistry {
    entities: Array<Entity<any>> = new Array<Entity<any>>();

    entityTypeToEntity: Map<EntityType, Entity<any>> = new Map<EntityType, Entity<any>>();

    collectionNameToEntityType: Map<string, EntityType> = new Map<string, EntityType>();

    pathNameToEntityType: Map<string, EntityType> = new Map<string, EntityType>();

    graphNameToEntityType: Map<string, EntityType> = new Map<string, EntityType>();

    register(entity: Entity<any>) {
        this.entities.push(entity);
        this.entityTypeToEntity.set(entity.type, entity);
        this.collectionNameToEntityType.set(entity.getCollectionName(), entity.type);
        this.pathNameToEntityType.set(entity.getPathName(), entity.type);
        this.graphNameToEntityType.set(entity.getGraphName(), entity.type);
    }

    getEntity(type: EntityType): Entity<any> {
        return validatedGet(type, this.entityTypeToEntity, DefaultEntity);
    }

    hasEntity(type: EntityType): boolean {
        return this.entityTypeToEntity.has(type);
    }

    getEntities(): Array<Entity<any>> {
        return this.entities;
    }

    getEntitiesForSearchRoutes(): Array<Entity<any>> {
        return this.entities.filter(
            (entity) => !GLOSSARY_ENTITY_TYPES.includes(entity.type) && entity.type !== EntityType.Domain,
        );
    }

    getNonGlossaryEntities(): Array<Entity<any>> {
        return this.entities.filter((entity) => !GLOSSARY_ENTITY_TYPES.includes(entity.type));
    }

    getGlossaryEntities(): Array<Entity<any>> {
        return this.entities.filter((entity) => GLOSSARY_ENTITY_TYPES.includes(entity.type));
    }

    getSearchEntityTypes(): Array<EntityType> {
        return this.entities.filter((entity) => entity.isSearchEnabled()).map((entity) => entity.type);
    }

    getDefaultSearchEntityType(): EntityType {
        return this.entities[0].type;
    }

    getBrowseEntityTypes(): Array<EntityType> {
        return this.entities.filter((entity) => entity.isBrowseEnabled()).map((entity) => entity.type);
    }

    getLineageEntityTypes(): Array<EntityType> {
        return this.entities.filter((entity) => entity.isLineageEnabled()).map((entity) => entity.type);
    }

    getIcon(type: EntityType, fontSize?: number, styleType?: IconStyleType, color?: string): JSX.Element {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.icon(fontSize, styleType || IconStyleType.TAB_VIEW, color);
    }

    getCollectionName(type: EntityType): string {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.getCollectionName();
    }

    getEntityName(type: EntityType): string | undefined {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.getEntityName?.();
    }

    getTypeFromCollectionName(name: string): EntityType {
        return validatedGet(name, this.collectionNameToEntityType, DefaultEntity.type);
    }

    getPathName(type: EntityType): string {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.getPathName();
    }

    getEntityUrl(type: EntityType, urn: string, params?: Record<string, string | boolean>): string {
        return `/${this.getPathName(type)}/${urlEncodeUrn(urn)}${params ? `?${dictToQueryStringParams(params)}` : ''}`;
    }

    getTypeFromPathName(pathName: string): EntityType {
        return validatedGet(pathName, this.pathNameToEntityType, DefaultEntity.type);
    }

    getTypeOrDefaultFromPathName(pathName: string, def?: EntityType): EntityType | undefined {
        return validatedGet(pathName, this.pathNameToEntityType, def);
    }

    renderProfile(type: EntityType, urn: string): JSX.Element {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.renderProfile(urn);
    }

    renderPreview<T>(entityType: EntityType, type: PreviewType, data: T, actions?: EntityMenuActions): JSX.Element {
        const entity = validatedGet(entityType, this.entityTypeToEntity, DefaultEntity);
        const genericEntityData = entity.getGenericEntityProperties(data);
        return (
            <PreviewContext.Provider value={genericEntityData}>
                {entity.renderPreview(type, data, actions)}
            </PreviewContext.Provider>
        );
    }

    renderSearchResult(type: EntityType, searchResult: SearchResult): JSX.Element {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        const genericEntityData = entity.getGenericEntityProperties(searchResult.entity);
        return (
            <SearchResultProvider searchResult={searchResult}>
                <PreviewContext.Provider value={genericEntityData}>
                    {entity.renderSearch(searchResult)}
                </PreviewContext.Provider>
            </SearchResultProvider>
        );
    }

    renderSearchMatches(type: EntityType, searchResult: SearchResult): JSX.Element {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return (
            <SearchResultProvider searchResult={searchResult}>
                {entity?.renderSearchMatches?.(searchResult) || <></>}
            </SearchResultProvider>
        );
    }

    renderBrowse<T>(type: EntityType, data: T): JSX.Element {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.renderPreview(PreviewType.BROWSE, data);
    }

    // render the regular profile if embedded profile doesn't exist. Compact context should be set to true.
    renderEmbeddedProfile(type: EntityType, urn: string): JSX.Element {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.renderEmbeddedProfile ? entity.renderEmbeddedProfile(urn) : entity.renderProfile(urn);
    }

    getLineageVizConfig<T>(type: EntityType, data: T): FetchedEntity {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        const genericEntityProperties = this.getGenericEntityProperties(type, data);
        // combine fineGrainedLineages from this node as well as its siblings
        const fineGrainedLineages = getFineGrainedLineageWithSiblings(
            genericEntityProperties,
            (t: EntityType, d: EntityInterface) => this.getGenericEntityProperties(t, d),
        );
        return {
            ...entity.getLineageVizConfig?.(data),
            downstreamChildren: genericEntityProperties?.downstream?.relationships
                ?.filter((relationship) => relationship.entity)
                ?.map((relationship) => ({
                    entity: relationship.entity as EntityInterface,
                    type: (relationship.entity as EntityInterface).type,
                })),
            downstreamRelationships: genericEntityProperties?.downstream?.relationships?.filter(
                (relationship) => relationship.entity,
            ),
            numDownstreamChildren:
                (genericEntityProperties?.downstream?.total || 0) -
                (genericEntityProperties?.downstream?.filtered || 0),
            upstreamChildren: genericEntityProperties?.upstream?.relationships
                ?.filter((relationship) => relationship.entity)
                ?.map((relationship) => ({
                    entity: relationship.entity as EntityInterface,
                    type: (relationship.entity as EntityInterface).type,
                })),
            upstreamRelationships: genericEntityProperties?.upstream?.relationships?.filter(
                (relationship) => relationship.entity,
            ),
            numUpstreamChildren:
                (genericEntityProperties?.upstream?.total || 0) - (genericEntityProperties?.upstream?.filtered || 0),
            status: genericEntityProperties?.status,
            siblingPlatforms: genericEntityProperties?.siblingPlatforms,
            fineGrainedLineages,
            siblings: genericEntityProperties?.siblings,
            schemaMetadata: genericEntityProperties?.schemaMetadata,
            inputFields: genericEntityProperties?.inputFields,
            canEditLineage: genericEntityProperties?.privileges?.canEditLineage,
        } as FetchedEntity;
    }

    getLineageVizConfigV2<T>(type: EntityType, data: T, flags?: FeatureFlagsConfig): FetchedEntityV2 | null {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        const genericEntityProperties = this.getGenericEntityProperties(type, data, flags);
        if (!genericEntityProperties || !entity.getLineageVizConfig) return null;

        const reversedBrowsePath = genericEntityProperties.browsePathV2?.path?.slice();
        reversedBrowsePath?.reverse();
        const containers = genericEntityProperties?.parentContainers?.containers?.length
            ? genericEntityProperties?.parentContainers?.containers
                  ?.map((p) => this.getGenericEntityProperties(p.type, p))
                  .filter((p): p is GenericEntityProperties => !!p)
            : reversedBrowsePath
                  ?.map((p) => (p.entity ? this.getGenericEntityProperties(p.entity.type, p.entity) : { name: p.name }))
                  .filter((p): p is GenericEntityProperties => !!p);

        return {
            ...entity.getLineageVizConfig(data),
            containers,
            fineGrainedLineages:
                genericEntityProperties?.fineGrainedLineages ||
                genericEntityProperties?.inputOutput?.fineGrainedLineages ||
                [],
            numDownstreamChildren:
                (genericEntityProperties.downstream?.total || 0) - (genericEntityProperties.downstream?.filtered || 0),
            numUpstreamChildren:
                (genericEntityProperties.upstream?.total || 0) - (genericEntityProperties.upstream?.filtered || 0),
            downstreamRelationships: genericEntityProperties.downstream?.relationships
                ?.map((r) => ({ ...r, urn: r.entity?.urn }))
                .filter((r): r is FetchedEntityV2Relationship => !!r.urn),
            upstreamRelationships: genericEntityProperties.upstream?.relationships
                ?.map((r) => ({ ...r, urn: r.entity?.urn }))
                .filter((r): r is FetchedEntityV2Relationship => !!r.urn),
            exists: genericEntityProperties.exists,
            health: genericEntityProperties.health ?? undefined,
            status: genericEntityProperties.status ?? undefined,
            schemaMetadata: genericEntityProperties.schemaMetadata ?? undefined,
            inputFields: genericEntityProperties.inputFields ?? undefined,
            canEditLineage: genericEntityProperties.privileges?.canEditLineage ?? undefined,
            lineageSiblingIcon: genericEntityProperties?.lineageSiblingIcon,
            structuredProperties: genericEntityProperties.structuredProperties ?? undefined,
        };
    }

    getLineageAssets(type: EntityType, data: EntityLineageV2Fragment): Map<string, LineageAsset> | undefined {
        // TODO: Fold into entity registry?
        if (data?.__typename === 'Domain') {
            return data?.dataProducts?.searchResults?.reduce((obj, r) => {
                if (r.entity.__typename === 'DataProduct') {
                    const name = this.getDisplayName(r.entity.type, r.entity);
                    obj.set(name, { name, type: LineageAssetType.DataProduct, size: r.entity.entities?.total });
                }
                return obj;
            }, new Map<string, LineageAsset>());
        }
        const fields = getSchemaFields(data, this.getGenericEntityProperties(type, data));
        if (fields) {
            return new Map(
                fields.map((field) => {
                    const name = downgradeV2FieldPath(field.fieldPath);
                    const value: LineageAsset = {
                        name,
                        type: LineageAssetType.Column,
                        dataType: field.type,
                        nativeDataType: field.nativeDataType,
                    };
                    return [name, value];
                }),
            );
        }
        return undefined;
    }

    getDisplayName<T>(type: EntityType, data: T): string {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.displayName(data);
    }

    getSidebarTabs(type: EntityType): EntitySidebarTab[] {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.getSidebarTabs ? entity.getSidebarTabs() : [];
    }

    getSidebarSections(type: EntityType): EntitySidebarSection[] {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.getSidebarSections ? entity.getSidebarSections() : [];
    }

    getGenericEntityProperties<T>(
        type: EntityType,
        data: T,
        flags?: FeatureFlagsConfig,
    ): GenericEntityProperties | null {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.getGenericEntityProperties(data, flags);
    }

    getSupportedEntityCapabilities(type: EntityType): Set<EntityCapabilityType> {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.supportedCapabilities();
    }

    getTypesWithSupportedCapabilities(capability: EntityCapabilityType): Set<EntityType> {
        return new Set(
            this.getEntities()
                .filter((entity) => entity.supportedCapabilities().has(capability))
                .map((entity) => entity.type),
        );
    }

    getCustomCardUrlPath(type: EntityType): string | undefined {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.getCustomCardUrlPath?.() as string | undefined;
    }

    getTypeFromGraphName(name: string): EntityType | undefined {
        return this.graphNameToEntityType.get(name);
    }

    getGraphNameFromType(type: EntityType): string {
        return validatedGet(type, this.entityTypeToEntity, DefaultEntity).getGraphName();
    }

    getEntityQuery(type: EntityType):
        | ((
              baseOptions: QueryHookOptions<
                  any,
                  Exact<{
                      urn: string;
                  }>
              >,
          ) => QueryResult<
              any,
              Exact<{
                  urn: string;
              }>
          >)
        | undefined {
        const entity = validatedGet(type, this.entityTypeToEntity, DefaultEntity);
        return entity.useEntityQuery;
    }
}

function getSchemaFields(
    data: EntityLineageV2Fragment,
    genericEntityProperties: GenericEntityProperties | null,
): LineageSchemaFieldFragment[] | undefined {
    if (data?.__typename === 'Dataset') {
        return data?.schemaMetadata?.fields;
    }
    if (data?.__typename === 'Chart') {
        return data?.inputFields?.fields
            ?.map((field) => field?.schemaField)
            .filter((field): field is LineageSchemaFieldFragment => !!field);
    }
    return genericEntityProperties?.schemaMetadata?.fields;
}
