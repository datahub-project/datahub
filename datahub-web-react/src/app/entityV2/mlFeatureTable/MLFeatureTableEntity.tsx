import { ChartScatter } from '@phosphor-icons/react/dist/csr/ChartScatter';
import { Database } from '@phosphor-icons/react/dist/csr/Database';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { Table } from '@phosphor-icons/react/dist/csr/Table';
import { WarningCircle } from '@phosphor-icons/react/dist/csr/WarningCircle';
import i18next from 'i18next';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/mlFeatureTable/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import { getDataProduct, isOutputPort } from '@app/entityV2/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { useGetMlFeatureTableQuery } from '@graphql/mlFeatureTable.generated';
import { EntityType, MlFeatureTable, SearchResult } from '@types';

const Sources = lazyProfileComponent('Sources', () => import('@app/entityV2/mlFeatureTable/profile/Sources'));
const MlFeatureTableFeatures = lazyProfileComponent(
    'MlFeatureTableFeatures',
    () => import('@app/entityV2/mlFeatureTable/profile/features/MlFeatureTableFeatures'),
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
const IncidentTab = lazyProfileComponent('IncidentTab', () =>
    import('@app/entityV2/shared/tabs/Incident/IncidentTab').then((module) => ({
        default: module.IncidentTab,
    })),
);
const PropertiesTab = lazyProfileComponent('PropertiesTab', () =>
    import('@app/entityV2/shared/tabs/Properties/PropertiesTab').then((module) => ({
        default: module.PropertiesTab,
    })),
);

const headerDropdownItems = new Set([
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.RAISE_INCIDENT,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub MLFeatureTable entity.
 */
export class MLFeatureTableEntity implements Entity<MlFeatureTable> {
    type: EntityType = EntityType.MlfeatureTable;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <ChartScatter
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

    getGraphName = () => 'mlFeatureTable';

    getPathName = () => 'featureTables';

    getEntityName = () => i18next.t('entity.types:mlFeatureTable.name');

    getCollectionName = () => i18next.t('entity.types:mlFeatureTable.namePlural');

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
                    name: i18next.t('entity.types:mlFeature.namePlural'),
                    component: MlFeatureTableFeatures,
                    icon: Table,
                },
                {
                    name: i18next.t('entity.types:mlFeatureTable.sourcesTab'),
                    component: Sources,
                    icon: Database,
                },
                {
                    name: i18next.t('entity.types:tab.documentation'),
                    component: DocumentationTab,
                    icon: FileText,
                },
                {
                    name: i18next.t('entity.types:tab.properties'),
                    component: PropertiesTab,
                    icon: ListBullets,
                },
                {
                    name: i18next.t('entity.types:tab.incidents'),
                    icon: WarningCircle,
                    component: IncidentTab,
                    getCount: (_, mlFeatureTable) => {
                        return mlFeatureTable?.mlFeatureTable?.activeIncidents?.total;
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
            name: i18next.t('entity.types:tab.properties'),
            component: PropertiesTab,
            description: i18next.t('entity.types:sidebar.propertiesDescription'),
            icon: ListBullets,
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
                previewType={PreviewType.SEARCH}
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
            EntityCapabilityType.APPLICATIONS,
            EntityCapabilityType.RELATED_DOCUMENTS,
            EntityCapabilityType.FORMS,
        ]);
    };

    getPlatformProperties = (data: MlFeatureTable) => {
        return data?.platform;
    };
}
