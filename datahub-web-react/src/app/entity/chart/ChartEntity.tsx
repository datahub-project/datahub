import { LineChartOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Chart, EntityType, LineageDirection, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { ChartPreview } from './preview/ChartPreview';
import { GetChartQuery, useGetChartQuery, useUpdateChartMutation } from '../../../graphql/chart.generated';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { GenericEntityProperties } from '../shared/types';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { ChartDashboardsTab } from '../shared/tabs/Entity/ChartDashboardsTab';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';
import { ChartStatsSummarySubHeader } from './profile/stats/ChartStatsSummarySubHeader';
import { InputFieldsTab } from '../shared/tabs/Entity/InputFieldsTab';
import { EmbedTab } from '../shared/tabs/Embed/EmbedTab';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { getDataProduct } from '../shared/utils';
import EmbeddedProfile from '../shared/embed/EmbeddedProfile';
import { LOOKER_URN } from '../../ingest/source/builder/constants';
import { MatchedFieldList } from '../../search/matches/MatchedFieldList';
import { matchedInputFieldRenderer } from '../../search/matches/matchedInputFieldRenderer';

/**
 * Definition of the DataHub Chart entity.
 */
export class ChartEntity implements Entity<Chart> {
    type: EntityType = EntityType.Chart;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <LineChartOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <LineChartOutlined style={{ fontSize, color: color || 'rgb(144 163 236)' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M888 792H200V168c0-4.4-3.6-8-8-8h-56c-4.4 0-8 3.6-8 8v688c0 4.4 3.6 8 8 8h752c4.4 0 8-3.6 8-8v-56c0-4.4-3.6-8-8-8zM305.8 637.7c3.1 3.1 8.1 3.1 11.3 0l138.3-137.6L583 628.5c3.1 3.1 8.2 3.1 11.3 0l275.4-275.3c3.1-3.1 3.1-8.2 0-11.3l-39.6-39.6a8.03 8.03 0 00-11.3 0l-230 229.9L461.4 404a8.03 8.03 0 00-11.3 0L266.3 586.7a8.03 8.03 0 000 11.3l39.5 39.7z" />
            );
        }

        return (
            <LineChartOutlined
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

    getAutoCompleteFieldName = () => 'title';

    getPathName = () => 'chart';

    getEntityName = () => 'Chart';

    getCollectionName = () => 'Charts';

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Chart}
            useEntityQuery={useGetChartQuery}
            useUpdateQuery={useUpdateChartMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.UPDATE_DEPRECATION])}
            subHeader={{
                component: ChartStatsSummarySubHeader,
            }}
            tabs={[
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Fields',
                    component: InputFieldsTab,
                    display: {
                        visible: (_, chart: GetChartQuery) => (chart?.chart?.inputFields?.fields?.length || 0) > 0,
                        enabled: (_, chart: GetChartQuery) => (chart?.chart?.inputFields?.fields?.length || 0) > 0,
                    },
                },
                {
                    name: 'Preview',
                    component: EmbedTab,
                    display: {
                        visible: (_, chart: GetChartQuery) =>
                            !!chart?.chart?.embed?.renderUrl && chart?.chart?.platform.urn === LOOKER_URN,
                        enabled: (_, chart: GetChartQuery) =>
                            !!chart?.chart?.embed?.renderUrl && chart?.chart?.platform.urn === LOOKER_URN,
                    },
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                    properties: {
                        defaultDirection: LineageDirection.Upstream,
                    },
                },
                {
                    name: 'Dashboards',
                    component: ChartDashboardsTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, chart: GetChartQuery) => (chart?.chart?.dashboards?.total || 0) > 0,
                    },
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
            ]}
            sidebarSections={[
                {
                    component: SidebarAboutSection,
                },
                {
                    component: SidebarOwnerSection,
                },
                {
                    component: SidebarTagsSection,
                    properties: {
                        hasTags: true,
                        hasTerms: true,
                    },
                },
                {
                    component: SidebarDomainSection,
                },
                {
                    component: DataProductSection,
                },
            ]}
        />
    );

    getOverridePropertiesFromEntity = (chart?: Chart | null): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const name = chart?.properties?.name;
        const subTypes = chart?.subTypes;
        const externalUrl = chart?.properties?.externalUrl;
        return {
            name,
            externalUrl,
            entityTypeOverride: subTypes ? capitalizeFirstLetterOnly(subTypes.typeNames?.[0]) : '',
        };
    };

    renderPreview = (_: PreviewType, data: Chart) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <ChartPreview
                urn={data.urn}
                subType={data.subTypes?.typeNames?.[0]}
                platform={data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)}
                name={data.properties?.name}
                description={data.editableProperties?.description || data.properties?.description}
                access={data.properties?.access}
                owners={data.ownership?.owners}
                tags={data?.globalTags || undefined}
                glossaryTerms={data?.glossaryTerms}
                logoUrl={data?.platform?.properties?.logoUrl}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                parentContainers={data.parentContainers}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Chart;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <ChartPreview
                urn={data.urn}
                subType={data.subTypes?.typeNames?.[0]}
                platform={data?.platform?.properties?.displayName || capitalizeFirstLetterOnly(data?.platform?.name)}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                name={data.properties?.name}
                description={data.editableProperties?.description || data.properties?.description}
                access={data.properties?.access}
                owners={data.ownership?.owners}
                tags={data?.globalTags || undefined}
                glossaryTerms={data?.glossaryTerms}
                insights={result.insights}
                logoUrl={data?.platform?.properties?.logoUrl || ''}
                domain={data.domain?.domain}
                dataProduct={getDataProduct(genericProperties?.dataProduct)}
                deprecation={data.deprecation}
                statsSummary={data.statsSummary}
                lastUpdatedMs={data.properties?.lastModified?.time}
                createdMs={data.properties?.created?.time}
                externalUrl={data.properties?.externalUrl}
                snippet={
                    <MatchedFieldList
                        customFieldRenderer={(matchedField) => matchedInputFieldRenderer(matchedField, data)}
                    />
                }
                degree={(result as any).degree}
                paths={(result as any).paths}
            />
        );
    };

    getLineageVizConfig = (entity: Chart) => {
        return {
            urn: entity.urn,
            name: entity.properties?.name || entity.urn,
            type: EntityType.Chart,
            icon: entity?.platform?.properties?.logoUrl || undefined,
            platform: entity?.platform,
            subtype: entity?.subTypes?.typeNames?.[0] || undefined,
        };
    };

    displayName = (data: Chart) => {
        return data.properties?.name || data.urn;
    };

    getGenericEntityProperties = (data: Chart) => {
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
            EntityCapabilityType.DEPRECATION,
            EntityCapabilityType.SOFT_DELETE,
            EntityCapabilityType.DATA_PRODUCTS,
        ]);
    };

    renderEmbeddedProfile = (urn: string) => (
        <EmbeddedProfile
            urn={urn}
            entityType={EntityType.Chart}
            useEntityQuery={useGetChartQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
        />
    );
}
