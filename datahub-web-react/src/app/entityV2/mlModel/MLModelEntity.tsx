import { PartitionOutlined, WarningOutlined } from '@ant-design/icons';
import { Cube, ListBullets, TreeStructure } from '@phosphor-icons/react';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/mlModel/preview/Preview';
import MLModelGroupsTab from '@app/entityV2/mlModel/profile/MLModelGroupsTab';
import MLModelSummary from '@app/entityV2/mlModel/profile/MLModelSummary';
import MlModelFeaturesTab from '@app/entityV2/mlModel/profile/MlModelFeaturesTab';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarApplicationSection } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import SidebarNotesSection from '@app/entityV2/shared/sidebarSection/SidebarNotesSection';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { IncidentTab } from '@app/entityV2/shared/tabs/Incident/IncidentTab';
import { LineageTab } from '@app/entityV2/shared/tabs/Lineage/LineageTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { SidebarTitleActionType, isOutputPort } from '@app/entityV2/shared/utils';

import { useGetMlModelQuery } from '@graphql/mlModel.generated';
import { EntityType, MlModel, SearchResult } from '@types';

const headerDropdownItems = new Set([
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.RAISE_INCIDENT,
    EntityMenuItems.ANNOUNCE,
    EntityMenuItems.LINK_VERSION,
]);

/**
 * Definition of the DataHub MlModel entity.
 */
export class MLModelEntity implements Entity<MlModel> {
    type: EntityType = EntityType.Mlmodel;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <Cube
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
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
            // eslint-disable-next-line @typescript-eslint/dot-notation
            name: mlModel && this.displayName(mlModel),
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
                    name: 'Lineage',
                    component: LineageTab,
                    icon: PartitionOutlined,
                    supportsFullsize: true,
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
                    getCount: (_, mlModel) => {
                        return mlModel?.mlModel?.activeIncidents?.total;
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
            component: SidebarApplicationSection,
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
            name: 'Lineage',
            component: LineageTab,
            description: "View this data asset's upstream and downstream dependencies",
            icon: TreeStructure,
            properties: {
                actionType: SidebarTitleActionType.LineageExplore,
            },
        },
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
            icon: ListBullets,
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
                previewType={PreviewType.SEARCH}
            />
        );
    };

    getLineageVizConfig = (entity: MlModel) => {
        return {
            urn: entity.urn,
            name: entity && this.displayName(entity),
            type: EntityType.Mlmodel,
            icon: entity.platform?.properties?.logoUrl || undefined,
            platform: entity.platform,
            deprecation: entity?.deprecation,
        };
    };

    displayName = (data: MlModel) => {
        // eslint-disable-next-line @typescript-eslint/dot-notation
        return data.properties?.['propertiesName'] || data.properties?.name || data.name || data.urn;
    };

    getGenericEntityProperties = (mlModel: MlModel) => {
        return getDataForEntityType({
            data: mlModel,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
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
            EntityCapabilityType.APPLICATIONS,
            EntityCapabilityType.RELATED_DOCUMENTS,
            EntityCapabilityType.FORMS,
        ]);
    };
}
