import * as React from 'react';
import { ConsoleSqlOutlined } from '@ant-design/icons';
import { DataJob, EntityType, PlatformType, RelationshipDirection, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getChildrenFromRelationships } from '../../lineage/utils/getChildren';
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
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { capitalizeFirstLetter } from '../../shared/capitalizeFirstLetter';

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
                            (dataJob?.dataJob?.incoming?.count || 0) !== 0 ||
                            (dataJob?.dataJob?.outgoing?.count || 0) !== 0,
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
            ]}
        />
    );

    getOverridePropertiesFromEntity = (dataJob?: DataJob | null): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const tool = dataJob?.dataFlow?.orchestrator || '';
        const name = dataJob?.properties?.name;
        const externalUrl = dataJob?.properties?.externalUrl;
        return {
            name,
            externalUrl,
            platform: {
                urn: `urn:li:dataPlatform:(${tool})`,
                type: EntityType.DataPlatform,
                name: tool,
                info: {
                    logoUrl: getLogoFromPlatform(tool),
                    displayName: capitalizeFirstLetter(tool),
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
                name={data.properties?.name || ''}
                description={data.editableProperties?.description || data.properties?.description}
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
                name={data.properties?.name || ''}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={platformName}
                platformLogo={getLogoFromPlatform(data.dataFlow?.orchestrator || '')}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                insights={result.insights}
            />
        );
    };

    getLineageVizConfig = (entity: DataJob) => {
        return {
            urn: entity?.urn,
            name: entity?.properties?.name || '',
            type: EntityType.DataJob,
            downstreamChildren: getChildrenFromRelationships({
                // eslint-disable-next-line @typescript-eslint/dot-notation
                incomingRelationships: entity?.['incoming'],
                // eslint-disable-next-line @typescript-eslint/dot-notation
                outgoingRelationships: entity?.['outgoing'],
                direction: RelationshipDirection.Incoming,
            }),
            upstreamChildren: getChildrenFromRelationships({
                // eslint-disable-next-line @typescript-eslint/dot-notation
                incomingRelationships: entity?.['incoming'],
                // eslint-disable-next-line @typescript-eslint/dot-notation
                outgoingRelationships: entity?.['outgoing'],
                direction: RelationshipDirection.Outgoing,
            }),
            icon: getLogoFromPlatform(entity.dataFlow?.orchestrator || ''),
            platform: entity?.dataFlow?.orchestrator || '',
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
