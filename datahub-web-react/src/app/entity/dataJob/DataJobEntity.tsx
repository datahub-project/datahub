import * as React from 'react';
import { ConsoleSqlOutlined } from '@ant-design/icons';
import { DataJob, EntityType, OwnershipType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { GetDataJobQuery, useGetDataJobQuery, useUpdateDataJobMutation } from '../../../graphql/dataJob.generated';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { GenericEntityProperties } from '../shared/types';
import { DataJobFlowTab } from '../shared/tabs/Entity/DataJobFlowTab';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { RunsTab } from './tabs/RunsTab';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';

const getDataJobPlatformName = (data?: DataJob): string => {
    return data?.dataFlow?.platform?.properties?.displayName || data?.dataFlow?.platform?.name || '';
};

/**
 * Definition of the DataHub DataJob entity.
 */
export class DataJobEntity implements Entity<DataJob> {
    type: EntityType = EntityType.DataJob;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ConsoleSqlOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <ConsoleSqlOutlined style={{ fontSize, color: '#B37FEB' }} />;
        }

        return (
            <ConsoleSqlOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'tasks';

    getEntityName = () => 'Task';

    getCollectionName = () => 'Tasks';

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.DataJob}
            useEntityQuery={useGetDataJobQuery}
            useUpdateQuery={useUpdateDataJobMutation}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.COPY_URL, EntityMenuItems.UPDATE_DEPRECATION])}
            tabs={[
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
                {
                    name: 'Pipeline',
                    component: DataJobFlowTab,
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, dataJob: GetDataJobQuery) =>
                            (dataJob?.dataJob?.upstream?.count || 0) !== 0 ||
                            (dataJob?.dataJob?.downstream?.count || 0) !== 0,
                    },
                },
                {
                    name: 'Runs',
                    component: RunsTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, dataJob: GetDataJobQuery) => (dataJob?.dataJob?.runs?.total || 0) !== 0,
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
                    properties: {
                        defaultOwnerType: OwnershipType.TechnicalOwner,
                    },
                },
                {
                    component: SidebarDomainSection,
                },
            ]}
        />
    );

    getOverridePropertiesFromEntity = (dataJob?: DataJob | null): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const name = dataJob?.properties?.name;
        const externalUrl = dataJob?.properties?.externalUrl;
        return {
            name,
            externalUrl,
            platform: dataJob?.dataFlow?.platform,
        };
    };

    renderPreview = (_: PreviewType, data: DataJob) => {
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || ''}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={getDataJobPlatformName(data)}
                platformLogo={data?.dataFlow?.platform?.properties?.logoUrl || ''}
                owners={data.ownership?.owners}
                globalTags={data.globalTags || null}
                domain={data.domain?.domain}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataJob;
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || ''}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={getDataJobPlatformName(data)}
                platformLogo={data?.dataFlow?.platform?.properties?.logoUrl || ''}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                domain={data.domain?.domain}
                insights={result.insights}
            />
        );
    };

    getLineageVizConfig = (entity: DataJob) => {
        return {
            urn: entity?.urn,
            name: entity?.properties?.name || '',
            type: EntityType.DataJob,
            icon: entity?.dataFlow?.platform?.properties?.logoUrl || '',
            platform: getDataJobPlatformName(entity),
        };
    };

    displayName = (data: DataJob) => {
        return data.properties?.name || data.urn;
    };

    getGenericEntityProperties = (data: DataJob) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };
}
