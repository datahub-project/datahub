import { CodeSandboxOutlined, UnorderedListOutlined, WarningOutlined } from '@ant-design/icons';
import * as React from 'react';
import { useGetMlModelQuery } from '../../../graphql/mlModel.generated';
import { EntityType, MlModel, SearchResult } from '../../../types.generated';
import { GenericEntityProperties } from '../../entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '../shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '../shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import SidebarStructuredProperties from '../shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import TabNameWithCount from '../shared/tabs/Entity/TabNameWithCount';
import { IncidentTab } from '../shared/tabs/Incident/IncidentTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { isOutputPort } from '../shared/utils';
import { Preview } from './preview/Preview';
import MLModelGroupsTab from './profile/MLModelGroupsTab';
import MLModelSummary from './profile/MLModelSummary';
import MlModelFeaturesTab from './profile/MlModelFeaturesTab';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';

const headerDropdownItems = new Set([
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.RAISE_INCIDENT,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub MlModel entity.
 */
export class MLModelEntity implements Entity<MlModel> {
    type: EntityType = EntityType.Mlmodel;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <CodeSandboxOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <CodeSandboxOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#9633b9' }} />
            );
        }

        return (
            <CodeSandboxOutlined
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

    getGraphName = () => 'mlModel';

    getPathName = () => 'mlModels';

    getEntityName = () => 'ML Model';

    getCollectionName = () => 'ML Models';

    getOverridePropertiesFromEntity = (mlModel?: MlModel | null): GenericEntityProperties => {
        return {
            externalUrl: mlModel?.properties?.externalUrl,
        };
    };

    useEntityQuery = useGetMlModelQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.Mlmodel}
            useEntityQuery={useGetMlModelQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            tabs={[
                {
                    name: 'Summary',
                    component: MLModelSummary,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
                {
                    name: 'Group',
                    component: MLModelGroupsTab,
                },
                {
                    name: 'Features',
                    component: MlModelFeaturesTab,
                },
                {
                    name: 'Incidents',
                    icon: WarningOutlined,
                    component: IncidentTab,
                    getDynamicName: (_, mlModel, loading) => {
                        const activeIncidentCount = mlModel?.mlModel?.activeIncidents?.total;
                        return <TabNameWithCount name="Incidents" count={activeIncidentCount} loading={loading} />;
                    },
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
            component: StatusSection,
        },
        {
            component: SidebarStructuredProperties,
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

    renderPreview = (previewType: PreviewType, data: MlModel) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                data={genericProperties}
                model={data}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlModel;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                data={genericProperties}
                model={data}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
            />
        );
    };

    getLineageVizConfig = (entity: MlModel) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.Mlmodel,
            icon: entity.platform?.properties?.logoUrl || undefined,
            platform: entity.platform,
            deprecation: entity?.deprecation,
        };
    };

    displayName = (data: MlModel) => {
        return data.name || data.urn;
    };

    getGenericEntityProperties = (mlModel: MlModel) => {
        return getDataForEntityType({ data: mlModel, entityType: this.type, getOverrideProperties: (data) => data });
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