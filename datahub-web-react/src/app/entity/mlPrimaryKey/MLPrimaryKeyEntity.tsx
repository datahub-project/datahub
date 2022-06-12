import * as React from 'react';
import { DotChartOutlined } from '@ant-design/icons';
import { MlPrimaryKey, EntityType, SearchResult, OwnershipType } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { GenericEntityProperties } from '../shared/types';
import { GetMlPrimaryKeyQuery, useGetMlPrimaryKeyQuery } from '../../../graphql/mlPrimaryKey.generated';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { FeatureTableTab } from '../shared/tabs/ML/MlPrimaryKeyFeatureTableTab';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/SidebarAboutSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';

/**
 * Definition of the DataHub MLPrimaryKey entity.
 */
export class MLPrimaryKeyEntity implements Entity<MlPrimaryKey> {
    type: EntityType = EntityType.MlprimaryKey;

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

    getPathName = () => 'mlPrimaryKeys';

    getEntityName = () => 'ML Primary Key';

    getCollectionName = () => 'ML Primary Keys';

    getOverridePropertiesFromEntity = (key?: MlPrimaryKey | null): GenericEntityProperties => {
        return {
            // eslint-disable-next-line
            platform: key?.['featureTables']?.relationships?.[0]?.entity?.platform,
        };
    };

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.MlprimaryKey}
            useEntityQuery={useGetMlPrimaryKeyQuery}
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
                {
                    name: 'Lineage',
                    component: LineageTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, result: GetMlPrimaryKeyQuery) => {
                            return (
                                (result?.mlPrimaryKey?.upstream?.total || 0) > 0 ||
                                (result?.mlPrimaryKey?.downstream?.total || 0) > 0
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

    renderPreview = (_: PreviewType, data: MlPrimaryKey) => {
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
        const data = result.entity as MlPrimaryKey;
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

    displayName = (data: MlPrimaryKey) => {
        return data.name;
    };

    getGenericEntityProperties = (mlPrimaryKey: MlPrimaryKey) => {
        return getDataForEntityType({
            data: mlPrimaryKey,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    getLineageVizConfig = (entity: MlPrimaryKey) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.MlprimaryKey,
            // eslint-disable-next-line
            icon: entity?.['featureTables']?.relationships?.[0]?.entity?.platform?.properties?.logoUrl || undefined,
            // eslint-disable-next-line
            platform: entity?.['featureTables']?.relationships?.[0]?.entity?.platform?.name,
        };
    };
}
