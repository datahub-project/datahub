import * as React from 'react';
import { ConsoleSqlOutlined } from '@ant-design/icons';
import { DataJob, EntityType, PlatformType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import getChildren from '../../lineage/utils/getChildren';
import { Direction } from '../../lineage/types';
import { getLogoFromPlatform } from '../../shared/getLogoFromPlatform';
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
            getOverrideProperties={this.getOverrideProperties}
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
                    shouldHide: (_, dataJob: GetDataJobQuery) =>
                        (dataJob?.dataJob?.incoming?.count || 0) === 0 &&
                        (dataJob?.dataJob?.outgoing?.count || 0) === 0,
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
            ]}
        />
    );

    getOverrideProperties = (res: GetDataJobQuery): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const tool = res.dataJob?.dataFlow?.orchestrator || '';
        const name = res.dataJob?.info?.name;
        const externalUrl = res.dataJob?.info?.externalUrl;
        return {
            ...res,
            name,
            externalUrl,
            platform: {
                urn: `urn:li:dataPlatform:(${tool})`,
                type: EntityType.DataPlatform,
                name: tool,
                info: {
                    logoUrl: getLogoFromPlatform(tool),
                    type: PlatformType.Others,
                    datasetNameDelimiter: '.',
                },
            },
        };
    };

    renderPreview = (_: PreviewType, data: DataJob) => {
        const platformName = data.dataFlow
            ? data.dataFlow?.orchestrator.charAt(0).toUpperCase() + data.dataFlow?.orchestrator.slice(1)
            : '';
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.editableProperties?.description || data.info?.description}
                platformName={platformName}
                platformLogo={getLogoFromPlatform(data.dataFlow?.orchestrator || '')}
                owners={data.ownership?.owners}
                globalTags={data.globalTags || null}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataJob;
        const platformName = data.dataFlow
            ? data.dataFlow?.orchestrator.charAt(0).toUpperCase() + data.dataFlow?.orchestrator.slice(1)
            : '';
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.editableProperties?.description || data.info?.description}
                platformName={platformName}
                platformLogo={getLogoFromPlatform(data.dataFlow?.orchestrator || '')}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
            />
        );
    };

    getLineageVizConfig = (entity: DataJob) => {
        return {
            urn: entity?.urn,
            name: entity?.info?.name || '',
            type: EntityType.DataJob,
            upstreamChildren: getChildren({ entity, type: EntityType.DataJob }, Direction.Upstream).map(
                (child) => child.entity.urn,
            ),
            downstreamChildren: getChildren({ entity, type: EntityType.DataJob }, Direction.Downstream).map(
                (child) => child.entity.urn,
            ),
            icon: getLogoFromPlatform(entity.dataFlow?.orchestrator || ''),
            platform: entity?.dataFlow?.orchestrator || '',
        };
    };

    displayName = (data: DataJob) => {
        return data.info?.name || data.urn;
    };
}
