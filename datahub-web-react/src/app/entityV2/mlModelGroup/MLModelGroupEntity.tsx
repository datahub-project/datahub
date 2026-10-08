import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import i18next from 'i18next';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/mlModelGroup/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import { SidebarTitleActionType, isOutputPort } from '@app/entityV2/shared/utils';

import { useGetMlModelGroupQuery } from '@graphql/mlModelGroup.generated';
import { EntityType, MlModelGroup, SearchResult } from '@types';

const ModelGroupModels = lazyProfileComponent(
    'ModelGroupModels',
    () => import('@app/entityV2/mlModelGroup/profile/ModelGroupModels'),
);
const EntityProfile = lazyProfileComponent('EntityProfile', () =>
    import('@app/entityV2/shared/containers/profile/EntityProfile').then((module) => ({
        default: module.EntityProfile,
    })),
);
const SidebarAboutSection = lazyProfileComponent('SidebarAboutSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection').then((module) => ({
        default: module.SidebarAboutSection,
    })),
);
const SidebarApplicationSection = lazyProfileComponent('SidebarApplicationSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection').then((module) => ({
        default: module.SidebarApplicationSection,
    })),
);
const DataProductSection = lazyProfileComponent(
    'DataProductSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection'),
);
const SidebarDomainSection = lazyProfileComponent('SidebarDomainSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection').then((module) => ({
        default: module.SidebarDomainSection,
    })),
);
const SidebarOwnerSection = lazyProfileComponent('SidebarOwnerSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection').then((module) => ({
        default: module.SidebarOwnerSection,
    })),
);
const SidebarEntityHeader = lazyProfileComponent(
    'SidebarEntityHeader',
    () => import('@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader'),
);
const SidebarGlossaryTermsSection = lazyProfileComponent('SidebarGlossaryTermsSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection').then((module) => ({
        default: module.SidebarGlossaryTermsSection,
    })),
);
const SidebarTagsSection = lazyProfileComponent('SidebarTagsSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection').then((module) => ({
        default: module.SidebarTagsSection,
    })),
);
const StatusSection = lazyProfileComponent(
    'StatusSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection'),
);
const SidebarNotesSection = lazyProfileComponent(
    'SidebarNotesSection',
    () => import('@app/entityV2/shared/sidebarSection/SidebarNotesSection'),
);
const SidebarStructuredProperties = lazyProfileComponent(
    'SidebarStructuredProperties',
    () => import('@app/entityV2/shared/sidebarSection/SidebarStructuredProperties'),
);
const DocumentationTab = lazyProfileComponent('DocumentationTab', () =>
    import('@app/entityV2/shared/tabs/Documentation/DocumentationTab').then((module) => ({
        default: module.DocumentationTab,
    })),
);
const LineageTab = lazyProfileComponent('LineageTab', () =>
    import('@app/entityV2/shared/tabs/Lineage/LineageTab').then((module) => ({
        default: module.LineageTab,
    })),
);
const PropertiesTab = lazyProfileComponent('PropertiesTab', () =>
    import('@app/entityV2/shared/tabs/Properties/PropertiesTab').then((module) => ({
        default: module.PropertiesTab,
    })),
);

const headerDropdownItems = new Set([
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub MlModelGroup entity.
 */
export class MLModelGroupEntity implements Entity<MlModelGroup> {
    type: EntityType = EntityType.MlmodelGroup;

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

    getGraphName = () => 'mlModelGroup';

    getPathName = () => 'mlModelGroup';

    getEntityName = () => i18next.t('entity.types:mlModelGroup.name');

    getCollectionName = () => i18next.t('entity.types:mlModelGroup.namePlural');

    getOverridePropertiesFromEntity = (mlModelGroup?: MlModelGroup | null): GenericEntityProperties => {
        return {
            name: mlModelGroup && this.displayName(mlModelGroup),
        };
    };

    useEntityQuery = useGetMlModelGroupQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.MlmodelGroup}
            useEntityQuery={useGetMlModelGroupQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            tabs={[
                {
                    name: i18next.t('entity.types:mlModelGroup.modelsTab'),
                    component: ModelGroupModels,
                },
                {
                    name: i18next.t('entity.types:tab.documentation'),
                    component: DocumentationTab,
                },
                {
                    name: i18next.t('entity.types:tab.lineage'),
                    component: LineageTab,
                    icon: TreeStructure,
                    supportsFullsize: true,
                },
                {
                    name: i18next.t('entity.types:tab.properties'),
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
            name: i18next.t('entity.types:tab.lineage'),
            component: LineageTab,
            description: i18next.t('entity.types:sidebar.lineageDescription'),
            icon: TreeStructure,
            properties: {
                actionType: SidebarTitleActionType.LineageExplore,
            },
        },
        {
            name: i18next.t('entity.types:tab.properties'),
            component: PropertiesTab,
            description: i18next.t('entity.types:sidebar.propertiesDescription'),
            icon: ListBullets,
        },
    ];

    renderPreview = (previewType: PreviewType, data: MlModelGroup) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                data={genericProperties}
                group={data}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlModelGroup;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                data={genericProperties}
                group={data}
                degree={(result as any).degree}
                paths={(result as any).paths}
                isOutputPort={isOutputPort(result)}
                headerDropdownItems={headerDropdownItems}
                previewType={PreviewType.SEARCH}
            />
        );
    };

    getLineageVizConfig = (entity: MlModelGroup) => {
        return {
            urn: entity.urn,
            name: entity && this.displayName(entity),
            type: EntityType.MlmodelGroup,
            icon: entity.platform?.properties?.logoUrl || undefined,
            platform: entity.platform,
            deprecation: entity?.deprecation,
        };
    };

    displayName = (data: MlModelGroup) => {
        // eslint-disable-next-line @typescript-eslint/dot-notation
        return data.properties?.['propertiesName'] || data.properties?.name || data.name || data.urn;
    };

    createdTime = (data: MlModelGroup) => {
        return data?.properties?.created?.time || data?.properties?.createdAt;
    };

    getGenericEntityProperties = (mlModelGroup: MlModelGroup) => {
        return getDataForEntityType({
            data: mlModelGroup,
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

    getPlatformProperties = (data: MlModelGroup) => {
        return data?.platform;
    };
}
