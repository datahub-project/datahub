import * as React from 'react';
import { ConsoleSqlOutlined, FileOutlined } from '@ant-design/icons';
import { DataPlatform, EntityType, QueryEntity as Query } from '../../../types.generated';
import { Entity, IconStyleType } from '../Entity';
import { GenericEntityProperties } from '../../entity/shared/types';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { useGetQueryQuery } from '../../../graphql/query.generated';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import SidebarQueryUpdatedAtSection from '../shared/containers/profile/sidebar/Query/SidebarQueryUpdatedAtSection';
import SidebarQueryDescriptionSection from '../shared/containers/profile/sidebar/Query/SidebarQueryDescriptionSection';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import SidebarQueryOperationsSection from '../shared/containers/profile/sidebar/Query/SidebarQueryOperationsSection';
import SidebarQueryDefinitionSection from '../shared/containers/profile/sidebar/Query/SidebarQueryDefinitionSection';
import { SidebarQueryLogicSection } from '../shared/containers/profile/sidebar/SidebarLogicSection';

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

    renderProfile = (urn: string) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.Query}
                useEntityQuery={useGetQueryQuery}
                tabs={[
                    {
                        name: 'Documentation',
                        component: DocumentationTab,
                        icon: FileOutlined,
                    },
                ]}
                sidebarSections={[
                    { component: SidebarQueryUpdatedAtSection },
                    { component: SidebarQueryDefinitionSection },
                    { component: SidebarQueryLogicSection },
                    { component: SidebarQueryDescriptionSection },
                    { component: SidebarQueryOperationsSection },
                ]}
                sidebarTabs={[]}
                getOverrideProperties={() => ({})}
            />
        );
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
