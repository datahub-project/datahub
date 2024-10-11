import * as React from 'react';
import { DotChartOutlined, UnorderedListOutlined } from '@ant-design/icons';
import { MlFeatureTable, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { GenericEntityProperties } from '../../entity/shared/types';
import { useGetMlFeatureTableQuery } from '../../../graphql/mlFeatureTable.generated';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarGlossaryTermsSection } from '../shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import MlFeatureTableFeatures from './profile/features/MlFeatureTableFeatures';
import Sources from './profile/Sources';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { getDataProduct, isOutputPort } from '../shared/utils';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import SidebarEntityHeader from '../shared/containers/profile/sidebar/SidebarEntityHeader';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import SharingAssetSection from '../shared/containers/profile/sidebar/shared/SharingAssetSection';

const headerDropdownItems = new Set([EntityMenuItems.UPDATE_DEPRECATION]);

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
            component: StatusSection,
        },
        {
            component: SharingAssetSection,
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
            component: SidebarAboutSection,
        },
        {
            component: SidebarGlossaryTermsSection,
        },
        {
            component: SidebarTagsSection,
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
