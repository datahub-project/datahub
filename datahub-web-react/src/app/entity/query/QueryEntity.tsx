import { GenericEntityProperties } from '@app/entity/shared/types';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { ConsoleSqlOutlined } from '@ant-design/icons';
import { useGetQueryQuery } from '@graphql/query.generated';
import { DataPlatform, EntityType, QueryEntity as Query } from '@types';
import * as React from 'react';
import { Entity, IconStyleType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';

/**
 * Definition of the DataHub DataPlatformInstance entity.
 * Most of this still needs to be filled out.
 */
export class QueryEntity implements Entity<Query> {
    type: EntityType = EntityType.Query;

    icon = (fontSize?: number, _styleType?: IconStyleType, color?: string) => {
        return (
            <ConsoleSqlOutlined
                className={TYPE_ICON_CLASS_NAME}
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'query';

    getEntityName = () => 'Query';

    getCollectionName = () => 'Queries';

    useEntityQuery = useGetQueryQuery;

    renderProfile = (_urn: string) => {
        return <></>;
    };

    getOverridePropertiesFromEntity = (query?: Query | null): GenericEntityProperties => {
        return {
            name: query && this.displayName(query),
            platform: query?.platform,
        };
    };

    renderEmbeddedProfile = (_: string) => <></>;

    renderPreview = () => {
        return <></>;
    };

    renderSearch = () => {
        return <></>;
    };

    getLineageVizConfig = (query: Query) => {
        // TODO: Set up types better here
        const platform: DataPlatform | undefined = (query as any)?.queryPlatform;
        return {
            urn: query.urn,
            name: query.properties?.name || query.urn,
            type: EntityType.Query,
            icon: platform?.properties?.logoUrl || undefined,
            platform: platform || undefined,
        };
    };

    displayName = (data: Query) => {
        return data?.properties?.name || (data?.properties?.source === 'SYSTEM' && 'System Query') || data?.urn;
    };

    getGenericEntityProperties = (data: Query) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    supportedCapabilities = () => {
        return new Set([]);
    };

    getGraphName = () => {
        return 'query';
    };
}
