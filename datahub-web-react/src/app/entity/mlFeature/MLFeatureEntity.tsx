import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlFeature, EntityType, SearchResult, OwnershipType } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { GenericEntityProperties } from '../shared/types';
import { GetMlFeatureQuery, useGetMlFeatureQuery } from '../../../graphql/mlFeature.generated';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { FeatureTableTab } from '../shared/tabs/ML/MlFeatureFeatureTableTab';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';

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

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'features';

    getEntityName = () => 'Feature';

    getCollectionName = () => 'Features';

    getOverridePropertiesFromEntity = (feature?: MlFeature | null): GenericEntityProperties => {
        return {
            // eslint-disable-next-line
            platform: feature?.['featureTables']?.relationships?.[0]?.entity?.platform,
        };
    };

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.Mlfeature}
            useEntityQuery={useGetMlFeatureQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            showDeprecateOption
            tabs={[
                {
                    name: 'Feature Tables',
                    component: FeatureTableTab,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, result: GetMlFeatureQuery) => {
                            return (
                                (result?.mlFeature?.upstream?.total || 0) > 0 ||
                                (result?.mlFeature?.downstream?.total || 0) > 0
                            );
                        },
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

    renderPreview = (_: PreviewType, data: MlFeature) => {
        // eslint-disable-next-line
        const platform = data?.['featureTables']?.relationships?.[0]?.entity?.platform;
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description}
                owners={data.ownership?.owners}
                platform={platform}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlFeature;
        // eslint-disable-next-line
        const platform = data?.['featureTables']?.relationships?.[0]?.entity?.platform;
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                featureNamespace={data.featureNamespace || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
                platform={platform}
                platformInstanceId={data.dataPlatformInstance?.instanceId}
            />
        );
    };

    displayName = (data: MlFeature) => {
        return data.name;
    };

    getGenericEntityProperties = (mlFeature: MlFeature) => {
        return getDataForEntityType({
            data: mlFeature,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
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
