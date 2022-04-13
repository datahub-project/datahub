import * as React from 'react';
import { CodeSandboxOutlined } from '@ant-design/icons';
import { MlModel, EntityType, SearchResult, OwnershipType } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { useGetMlModelQuery } from '../../../graphql/mlModel.generated';
import { GenericEntityProperties } from '../shared/types';
import MLModelSummary from './profile/MLModelSummary';
import MLModelGroupsTab from './profile/MLModelGroupsTab';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import MlModelFeaturesTab from './profile/MlModelFeaturesTab';

/**
 * Definition of the DataHub MlModel entity.
 */
export class MLModelEntity implements Entity<MlModel> {
    type: EntityType = EntityType.Mlmodel;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <CodeSandboxOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <CodeSandboxOutlined style={{ fontSize, color: '#9633b9' }} />;
        }

        return (
            <CodeSandboxOutlined
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

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'mlModels';

    getEntityName = () => 'ML Model';

    getCollectionName = () => 'ML Models';

    getOverridePropertiesFromEntity = (_?: MlModel | null): GenericEntityProperties => {
        return {};
    };

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.Mlmodel}
            useEntityQuery={useGetMlModelQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            tabs={[
                {
                    name: 'Summary',
                    component: MLModelSummary,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Group',
                    component: MLModelGroupsTab,
                },
                {
                    name: 'Features',
                    component: MlModelFeaturesTab,
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

    renderPreview = (_: PreviewType, data: MlModel) => {
        return <Preview model={data} />;
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlModel;
        return <Preview model={data} />;
    };

    getLineageVizConfig = (entity: MlModel) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.Mlmodel,
            icon: entity.platform?.properties?.logoUrl || undefined,
            platform: entity.platform?.name,
        };
    };

    displayName = (data: MlModel) => {
        return data.name;
    };

    getGenericEntityProperties = (mlModel: MlModel) => {
        return getDataForEntityType({ data: mlModel, entityType: this.type, getOverrideProperties: (data) => data });
    };
}
