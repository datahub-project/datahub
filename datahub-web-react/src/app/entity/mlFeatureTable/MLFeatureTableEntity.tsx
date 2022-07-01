import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlFeatureTable, EntityType, SearchResult, OwnershipType } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { GenericEntityProperties } from '../shared/types';
import { useGetMlFeatureTableQuery } from '../../../graphql/mlFeatureTable.generated';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import MlFeatureTableFeatures from './profile/features/MlFeatureTableFeatures';
import Sources from './profile/Sources';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';

/**
 * Definition of the DataHub MLFeatureTable entity.
 */
export class MLFeatureTableEntity implements Entity<MlFeatureTable> {
    type: EntityType = EntityType.MlfeatureTable;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DotChartOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DotChartOutlined style={{ fontSize, color: '#9633b9' }} />;
        }

        return (
            <DotChartOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'featureTables';

    getEntityName = () => 'Feature Table';

    getCollectionName = () => 'Feature Tables';

    getOverridePropertiesFromEntity = (_?: MlFeatureTable | null): GenericEntityProperties => {
        return {};
    };

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.MlfeatureTable}
            useEntityQuery={useGetMlFeatureTableQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.COPY_URL, EntityMenuItems.UPDATE_DEPRECATION])}
            tabs={[
                {
                    name: 'Features',
                    component: MlFeatureTableFeatures,
                },
                {
                    name: 'Sources',
                    component: Sources,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
            ]}
            sidebarSections={[
                {
                    component: SidebarAboutSection,
                },
                {
                    component: SidebarTagsSection,
                    properties: {
                        hasTags: true,
                        hasTerms: true,
                    },
                },
                {
                    component: SidebarOwnerSection,
                    properties: {
                        defaultOwnerType: OwnershipType.TechnicalOwner,
                    },
                },
                {
                    component: SidebarDomainSection,
                },
            ]}
        />
    );

    renderPreview = (_: PreviewType, data: MlFeatureTable) => {
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                description={data.description}
                owners={data.ownership?.owners}
                logoUrl={data.platform?.properties?.logoUrl}
                platformName={data.platform?.displayName}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlFeatureTable;
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
                logoUrl={data.platform?.properties?.logoUrl}
                platformName={data.platform?.displayName}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
            />
        );
    };

    getLineageVizConfig = (entity: MlFeatureTable) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.MlfeatureTable,
            icon: entity.platform.properties?.logoUrl || undefined,
            platform: entity.platform.name,
        };
    };

    displayName = (data: MlFeatureTable) => {
        return data.name;
    };

    getGenericEntityProperties = (mlFeatureTable: MlFeatureTable) => {
        return getDataForEntityType({
            data: mlFeatureTable,
            entityType: this.type,
            getOverrideProperties: (data) => data,
        });
    };
}
