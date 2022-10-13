import { LineChartOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Chart, EntityType, LineageDirection, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { ChartPreview } from './preview/ChartPreview';
import { GetChartQuery, useGetChartQuery, useUpdateChartMutation } from '../../../graphql/chart.generated';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
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
import { ChartSnippet } from './ChartSnippet';

/**
 * Definition of the DataHub Chart entity.
 */
export class ChartEntity implements Entity<Chart> {
    type: EntityType = EntityType.Chart;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <LineChartOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <LineChartOutlined style={{ fontSize, color: 'rgb(144 163 236)' }} />;
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
                    color: '#BFBFBF',
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
            headerDropdownItems={new Set([EntityMenuItems.COPY_URL, EntityMenuItems.UPDATE_DEPRECATION])}
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
                    name: 'Properties',
                    component: PropertiesTab,
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                    properties: {
                        defaultDirection: LineageDirection.Upstream,
                    },
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, chart: GetChartQuery) => {
                            return (
                                (chart?.chart?.upstream?.total || 0) > 0 || (chart?.chart?.downstream?.total || 0) > 0
                            );
                        },
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
            ]}
            sidebarSections={[
                {
                    component: SidebarAboutSection,
                },
                {
                    component: SidebarTagsSection,
                    properties: {
                        hasTags: true,
                        hasTerms: true,
                    },
                },
                {
                    component: SidebarOwnerSection,
                },
                {
                    component: SidebarDomainSection,
                },
            ]}
        />
    );

    getOverridePropertiesFromEntity = (chart?: Chart | null): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const name = chart?.properties?.name;
        const externalUrl = chart?.properties?.externalUrl;
        return {
            name,
            externalUrl,
        };
    };

    renderPreview = (_: PreviewType, data: Chart) => {
        return (
            <ChartPreview
                urn={data.urn}
                platform={data.platform.properties?.displayName || data.platform.name}
                name={data.properties?.name}
                description={data.editableProperties?.description || data.properties?.description}
                access={data.properties?.access}
                owners={data.ownership?.owners}
                tags={data?.globalTags || undefined}
                glossaryTerms={data?.glossaryTerms}
                logoUrl={data?.platform?.properties?.logoUrl}
                domain={data.domain?.domain}
                parentContainers={data.parentContainers}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Chart;
        return (
            <ChartPreview
                urn={data.urn}
                platform={data.platform.properties?.displayName || data.platform.name}
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
                deprecation={data.deprecation}
                statsSummary={data.statsSummary}
                lastUpdatedMs={data.properties?.lastModified?.time}
                createdMs={data.properties?.created?.time}
                externalUrl={data.properties?.externalUrl}
                snippet={<ChartSnippet matchedFields={result.matchedFields} inputFields={data.inputFields} />}
            />
        );
    };

    getLineageVizConfig = (entity: Chart) => {
        return {
            urn: entity.urn,
            name: entity.properties?.name || '',
            type: EntityType.Chart,
            icon: entity?.platform?.properties?.logoUrl || '',
            platform: entity?.platform,
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
        ]);
    };
}
