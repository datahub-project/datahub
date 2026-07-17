import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import i18next from 'i18next';
import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import MetricHeader from '@app/entityV2/metric/profile/MetricHeader';
import MetricPreview from '@app/entityV2/metric/preview/MetricPreview';
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

import { useGetMetricQuery } from '@graphql/metric.generated';
import { EntityType, Metric, SearchResult } from '@types';

const headerDropdownItems = new Set([EntityMenuItems.SHARE]);

export class MetricEntity implements Entity<Metric> {
    type: EntityType = EntityType.Metric;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <Sigma
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

    getPathName = () => 'metric';

    getEntityName = () => i18next.t('entity.types:metric.name', 'Metric');

    getCollectionName = () => i18next.t('entity.types:metric.namePlural', 'Metrics');

    useEntityQuery = useGetMetricQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Metric}
            useEntityQuery={useGetMetricQuery as any}
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
                properties: {
                    hideEditDescription: true,
                    preContent: React.createElement(MetricHeader),
                },
            },
            {
                name: i18next.t('entity.types:tab.properties', 'Properties'),
                component: PropertiesTab,
            },
        ];
    };

    renderPreview = (previewType: PreviewType, data: Metric) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <MetricPreview
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
        const data = result.entity as Metric;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <MetricPreview
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

    displayName = (data: Metric) => {
        return data?.info?.name || data?.id || data?.urn;
    };

    getOverridePropertiesFromEntity = (data: Metric): GenericEntityProperties => {
        return {
            name: data?.info?.name,
            properties: {
                description: data?.info?.description ?? undefined,
            },
        };
    };

    getGenericEntityProperties = (data: Metric) => {
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

    getGraphName = () => 'metric';
}
