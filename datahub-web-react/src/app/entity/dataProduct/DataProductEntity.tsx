import * as React from 'react';
import { FileDoneOutlined } from '@ant-design/icons';
import { DataProduct, EntityType, OwnershipType, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { SidebarViewDefinitionSection } from '../shared/containers/profile/sidebar/Dataset/View/SidebarViewDefinitionSection';
import { GetDatasetQuery } from '../../../graphql/dataset.generated';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import { Preview } from './preview/Preview';
import { useGetDataProductQuery } from '../../../graphql/dataProduct.generated';
import { DataProductEntitiesTab } from './DataProductEntitiesTab';
import { EntityActionItem } from '../shared/entity/EntityActions';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityDropdown';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import SidebarStructuredPropsSection from '../shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';

/**
 * Definition of the DataHub Data Product entity.
 */
export class DataProductEntity implements Entity<DataProduct> {
    type: EntityType = EntityType.DataProduct;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FileDoneOutlined />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <FileDoneOutlined style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <FileDoneOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'dataProduct';

    getEntityName = () => 'Data Product';

    getCollectionName = () => 'Data Products';

    useEntityQuery = useGetDataProductQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.DataProduct}
            useEntityQuery={useGetDataProductQuery}
            useUpdateQuery={undefined}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerActionItems={new Set([EntityActionItem.BATCH_ADD_DATA_PRODUCT])}
            headerDropdownItems={new Set([EntityMenuItems.DELETE])}
            isNameEditable
            tabs={[
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Assets',
                    component: DataProductEntitiesTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarOwnerSection,
            properties: {
                defaultOwnerType: OwnershipType.TechnicalOwner,
            },
        },
        {
            component: SidebarViewDefinitionSection,
            display: {
                // to do - change when we have a GetDataProductQuery
                visible: (_, dataset: GetDatasetQuery) => (dataset?.dataset?.viewProperties?.logic && true) || false,
            },
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
            properties: {
                updateOnly: true,
            },
        },
        {
            component: SidebarStructuredPropsSection,
        },
    ];

    renderPreview = (_: PreviewType, data: DataProduct) => {
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || ''}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                globalTags={data.tags}
                glossaryTerms={data.glossaryTerms}
                domain={data.domain?.domain}
                entityCount={data?.entities?.total || undefined}
                externalUrl={data.properties?.externalUrl}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataProduct;
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || ''}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                globalTags={data.tags}
                glossaryTerms={data.glossaryTerms}
                domain={data.domain?.domain}
                entityCount={data?.entities?.total || undefined}
                externalUrl={data.properties?.externalUrl}
                degree={(result as any).degree}
                paths={(result as any).paths}
            />
        );
    };

    displayName = (data: DataProduct) => {
        return data?.properties?.name || data.urn;
    };

    getOverridePropertiesFromEntity = (data: DataProduct) => {
        const name = data?.properties?.name;
        const externalUrl = data?.properties?.externalUrl;
        const entityCount = data?.entities?.total || undefined;
        return {
            name,
            externalUrl,
            entityCount,
        };
    };

    getGenericEntityProperties = (data: DataProduct) => {
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
        ]);
    };

    getGraphName = () => {
        return 'dataProduct';
    };
}
