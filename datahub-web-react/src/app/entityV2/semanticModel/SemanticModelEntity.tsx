import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import i18next from 'i18next';
import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import SemanticModelPreview from '@app/entityV2/semanticModel/preview/SemanticModelPreview';
import { DefinitionTab } from '@app/entityV2/semanticModel/profile/DefinitionTab';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { EntityTab } from '@app/entityV2/shared/types';
import SummaryTab from '@app/entityV2/summary/SummaryTab';

import { useGetSemanticModelQuery } from '@graphql/semanticModel.generated';
import { EntityType, SearchResult, SemanticModel } from '@types';

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

    getProfileTabs = (): EntityTab[] => {
        return [
            {
                name: i18next.t('entity.types:tab.summary'),
                component: SummaryTab,
            },
            {
                name: i18next.t('entity.types:tab.definition'),
                component: DefinitionTab,
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
        ]);
    };

    getGraphName = () => 'semanticModel';
}
