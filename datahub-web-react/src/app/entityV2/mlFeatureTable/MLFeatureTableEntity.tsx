import { DotChartOutlined, UnorderedListOutlined } from '@ant-design/icons';
import * as React from 'react';
import { useGetMlFeatureTableQuery } from '../../../graphql/mlFeatureTable.generated';
import { EntityType, MlFeatureTable, SearchResult } from '../../../types.generated';
import { GenericEntityProperties } from '../../entity/shared/types';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import SidebarEntityHeader from '../shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '../shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import SidebarStructuredProperties from '../shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { getDataProduct, isOutputPort } from '../shared/utils';
import { Preview } from './preview/Preview';
import MlFeatureTableFeatures from './profile/features/MlFeatureTableFeatures';
import Sources from './profile/Sources';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';

const headerDropdownItems = new Set([EntityMenuItems.UPDATE_DEPRECATION, EntityMenuItems.ANNOUNCE]);

/**
 * Definition of the DataHub MLFeatureTable entity.
 */
export class MLFeatureTableEntity implements Entity<MlFeatureTable> {
    type: EntityType = EntityType.MlfeatureTable;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DotChartOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <DotChartOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#9633b9' }} />
            );
        }

        return (
            <DotChartOutlined
                className={TYPE_ICON_CLASS_NAME}
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'mlFeatureTable';

    getPathName = () => 'featureTables';

    getEntityName = () => 'Feature Table';

    getCollectionName = () => 'Feature Tables';

    getOverridePropertiesFromEntity = (_?: MlFeatureTable | null): GenericEntityProperties => {
        return {};
    };

    useEntityQuery = useGetMlFeatureTableQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.MlfeatureTable}
            useEntityQuery={useGetMlFeatureTableQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
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
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarEntityHeader,
        },
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarNotesSection,
        },
        {
            component: SidebarOwnerSection,
        },
        {
            component: SidebarDomainSection,
        },
        {
            component: DataProductSection,
        },
        {
            component: SidebarTagsSection,
        },
        {
            component: SidebarGlossaryTermsSection,
        },
        {
            component: SidebarStructuredProperties,
        },
        {
            component: StatusSection,
        },
    ];

    getSidebarTabs = () => [
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
            icon: UnorderedListOutlined,
        },
    ];

    renderPreview = (previewType: PreviewType, data: MlFeatureTable) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.name || ''}
                description={data.description}
                owners={data.ownership?.owners}
                logoUrl={data.platform?.properties?.logoUrl}
                platformName={data.platform?.properties?.displayName || capitalizeFirstLetterOnly(data.platform?.name)}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlFeatureTable;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.name || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
                logoUrl={data.platform?.properties?.logoUrl}
                platformName={data.platform?.properties?.displayName || capitalizeFirstLetterOnly(data.platform?.name)}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
            />
        );
    };

    getLineageVizConfig = (entity: MlFeatureTable) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.MlfeatureTable,
            icon: entity.platform.properties?.logoUrl || undefined,
            platform: entity.platform,
            deprecation: entity?.deprecation,
        };
    };

    displayName = (data: MlFeatureTable) => {
        return data.name || data.urn;
    };

    getGenericEntityProperties = (mlFeatureTable: MlFeatureTable) => {
        return getDataForEntityType({
            data: mlFeatureTable,
            entityType: this.type,
            getOverrideProperties: (data) => data,
        });
    };

    supportedCapabilities = () => {
        return new Set([
            EntityCapabilityType.OWNERS,
            EntityCapabilityType.GLOSSARY_TERMS,
            EntityCapabilityType.TAGS,
            EntityCapabilityType.DOMAINS,
            EntityCapabilityType.DEPRECATION,
            EntityCapabilityType.SOFT_DELETE,
            EntityCapabilityType.DATA_PRODUCTS,
            EntityCapabilityType.LINEAGE,
        ]);
    };
}
