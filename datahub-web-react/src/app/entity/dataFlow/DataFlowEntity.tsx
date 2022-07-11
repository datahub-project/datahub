import * as React from 'react';
import { ShareAltOutlined } from '@ant-design/icons';
import { DataFlow, EntityType, OwnershipType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { useGetDataFlowQuery, useUpdateDataFlowMutation } from '../../../graphql/dataFlow.generated';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { GenericEntityProperties } from '../shared/types';
import { DataFlowJobsTab } from '../shared/tabs/Entity/DataFlowJobsTab';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';

/**
 * Definition of the DataHub DataFlow entity.
 */
export class DataFlowEntity implements Entity<DataFlow> {
    type: EntityType = EntityType.DataFlow;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ShareAltOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <ShareAltOutlined style={{ fontSize, color: '#d6246c' }} />;
        }

        return (
            <ShareAltOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'pipelines';

    getEntityName = () => 'Pipeline';

    getCollectionName = () => 'Pipelines';

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.DataFlow}
            useEntityQuery={useGetDataFlowQuery}
            useUpdateQuery={useUpdateDataFlowMutation}
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
                    name: 'Tasks',
                    component: DataFlowJobsTab,
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

    getOverridePropertiesFromEntity = (dataFlow?: DataFlow | null): GenericEntityProperties => {
        // TODO: Get rid of this once we have correctly formed platform coming back.
        const name = dataFlow?.properties?.name;
        const externalUrl = dataFlow?.properties?.externalUrl;
        return {
            name,
            externalUrl,
        };
    };

    renderPreview = (_: PreviewType, data: DataFlow) => {
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || ''}
                description={data.editableProperties?.description || data.properties?.description}
                platformName={data.platform.properties?.displayName || data.platform.name}
                platformLogo={data?.platform?.properties?.logoUrl || ''}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                domain={data.domain}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataFlow;
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || ''}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
                description={data.editableProperties?.description || data.properties?.description || ''}
                platformName={data.platform.properties?.displayName || data.platform.name}
                platformLogo={data?.platform?.properties?.logoUrl || ''}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                insights={result.insights}
                domain={data.domain}
            />
        );
    };

    displayName = (data: DataFlow) => {
        return data.properties?.name || data.urn;
    };

    getGenericEntityProperties = (data: DataFlow) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };
}
