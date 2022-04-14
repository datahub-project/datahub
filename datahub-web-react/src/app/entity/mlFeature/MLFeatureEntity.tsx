import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlFeature, EntityType, SearchResult, OwnershipType } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { GenericEntityProperties } from '../shared/types';
import { useGetMlFeatureQuery } from '../../../graphql/mlFeature.generated';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { FeatureTableTab } from '../shared/tabs/ML/MlFeatureFeatureTableTab';

/**
 * Definition of the DataHub MLFeature entity.
 */
export class MLFeatureEntity implements Entity<MlFeature> {
    type: EntityType = EntityType.Mlfeature;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DotChartOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DotChartOutlined style={{ fontSize, color: '#9633b9' }} />;
        }

        return (
            <DotChartOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'features';

    getEntityName = () => 'Feature';

    getCollectionName = () => 'Features';

    getOverridePropertiesFromEntity = (_?: MlFeature | null): GenericEntityProperties => {
        return {};
    };

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.Mlfeature}
            useEntityQuery={useGetMlFeatureQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            tabs={[
                {
                    name: 'Feature Tables',
                    component: FeatureTableTab,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
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

    renderPreview = (_: PreviewType, data: MlFeature) => {
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description}
                owners={data.ownership?.owners}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlFeature;
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
            />
        );
    };

    displayName = (data: MlFeature) => {
        return data.name;
    };

    getGenericEntityProperties = (mlFeature: MlFeature) => {
        return getDataForEntityType({ data: mlFeature, entityType: this.type, getOverrideProperties: (data) => data });
    };

    getLineageVizConfig = (entity: MlFeature) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.Mlfeature,
            // eslint-disable-next-line
            icon: entity?.['featureTables']?.relationships?.[0]?.entity?.platform?.properties?.logoUrl || undefined,
            // eslint-disable-next-line
            platform: entity?.['featureTables']?.relationships?.[0]?.entity?.platform?.name,
        };
    };
}
