import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import i18next from 'i18next';
import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import SemanticModelPreview from '@app/entityV2/semanticModel/preview/SemanticModelPreview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import { EntitySidebarTab, EntityTab } from '@app/entityV2/shared/types';
import { SidebarTitleActionType } from '@app/entityV2/shared/utils';

import { useGetSemanticModelQuery } from '@graphql/semanticModel.generated';
import { EntityType, SearchResult, SemanticModel } from '@types';

const DefinitionTab = lazyProfileComponent('DefinitionTab', () =>
    import('@app/entityV2/semanticModel/profile/DefinitionTab').then((module) => ({
        default: module.DefinitionTab,
    })),
);
const EntityProfile = lazyProfileComponent('EntityProfile', () =>
    import('@app/entityV2/shared/containers/profile/EntityProfile').then((module) => ({
        default: module.EntityProfile,
    })),
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
const SidebarStructuredProperties = lazyProfileComponent(
    'SidebarStructuredProperties',
    () => import('@app/entityV2/shared/sidebarSection/SidebarStructuredProperties'),
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
const SummaryTab = lazyProfileComponent('SummaryTab', () => import('@app/entityV2/summary/SummaryTab'));

const headerDropdownItems = new Set([EntityMenuItems.SHARE]);

export class SemanticModelEntity implements Entity<SemanticModel> {
    type: EntityType = EntityType.SemanticModel;

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

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'semanticModel';

    getEntityName = () => i18next.t('entity.types:semanticModel.name', 'Semantic Model');

    getCollectionName = () => i18next.t('entity.types:semanticModel.namePlural', 'Semantic Models');

    useEntityQuery = useGetSemanticModelQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.SemanticModel}
            useEntityQuery={useGetSemanticModelQuery as any}
            headerDropdownItems={headerDropdownItems}
            tabs={this.getProfileTabs()}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarEntityHeader,
        },
        {
            component: SidebarOwnerSection,
        },
        {
            component: SidebarTagsSection,
        },
        {
            component: SidebarGlossaryTermsSection,
        },
        {
            component: SidebarDomainSection,
        },
        {
            component: SidebarStructuredProperties,
        },
    ];

    getSidebarTabs = (): EntitySidebarTab[] => [
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

    getProfileTabs = (): EntityTab[] => {
        return [
            {
                name: i18next.t('entity.types:tab.summary'),
                component: SummaryTab,
                properties: {
                    hideEditDescription: true,
                },
            },
            {
                name: i18next.t('entity.types:tab.definition'),
                component: DefinitionTab,
            },
            {
                name: i18next.t('entity.types:tab.lineage'),
                component: LineageTab,
            },
            {
                name: i18next.t('entity.types:tab.properties', 'Properties'),
                component: PropertiesTab,
            },
        ];
    };

    renderPreview = (previewType: PreviewType, data: SemanticModel) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <SemanticModelPreview
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                description={data?.info?.description}
                owners={data?.ownership?.owners}
                globalTags={data?.tags}
                glossaryTerms={data?.glossaryTerms}
                domain={data?.domain?.domain}
                deprecation={data?.deprecation}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as SemanticModel;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <SemanticModelPreview
                urn={data.urn}
                data={genericProperties}
                name={this.displayName(data)}
                description={data?.info?.description}
                owners={data?.ownership?.owners}
                globalTags={data?.tags}
                glossaryTerms={data?.glossaryTerms}
                domain={data?.domain?.domain}
                degree={(result as any).degree}
                paths={(result as any).paths}
                deprecation={data?.deprecation}
                headerDropdownItems={headerDropdownItems}
                previewType={PreviewType.SEARCH}
            />
        );
    };

    displayName = (data: SemanticModel) => {
        return data?.info?.name || data?.id || data?.urn;
    };

    getLineageVizConfig = (entity: SemanticModel) => {
        return {
            urn: entity?.urn,
            name: entity?.info?.name || entity?.id || entity?.urn,
            type: EntityType.SemanticModel,
            icon: entity?.platform?.properties?.logoUrl || undefined,
            platform: entity?.platform,
            deprecation: entity?.deprecation,
        };
    };

    getOverridePropertiesFromEntity = (data: SemanticModel): GenericEntityProperties => {
        return {
            name: data?.info?.name,
            properties: {
                description: data?.info?.description ?? undefined,
            },
        };
    };

    getGenericEntityProperties = (data: SemanticModel) => {
        return getDataForEntityType({
            data,
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
            EntityCapabilityType.LINEAGE,
        ]);
    };

    getGraphName = () => 'semanticModel';
}
