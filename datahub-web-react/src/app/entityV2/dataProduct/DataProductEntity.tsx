import {
    AppstoreOutlined,
    FileDoneOutlined,
    FileOutlined,
    ReadOutlined,
    UnorderedListOutlined,
} from '@ant-design/icons';
import * as React from 'react';
import { useGetDataProductQuery } from '../../../graphql/dataProduct.generated';
import { GetDatasetQuery } from '../../../graphql/dataset.generated';
import { DataProduct, EntityType, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { EntityProfileTab } from '../shared/constants';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarViewDefinitionSection } from '../shared/containers/profile/sidebar/Dataset/View/SidebarViewDefinitionSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '../shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '../shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityActionItem } from '../shared/entity/EntityActions';
import SidebarStructuredProperties from '../shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import TabNameWithCount from '../shared/tabs/Entity/TabNameWithCount';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { DataProductEntitiesTab } from './DataProductEntitiesTab';
import { DataProductSummaryTab } from './DataProductSummaryTab';
import { Preview } from './preview/Preview';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';

const headerDropdownItems = new Set([
    EntityMenuItems.SHARE,
    EntityMenuItems.DELETE,
    EntityMenuItems.EDIT,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub Data Product entity.
 */
export class DataProductEntity implements Entity<DataProduct> {
    type: EntityType = EntityType.DataProduct;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FileDoneOutlined className={TYPE_ICON_CLASS_NAME} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <FileDoneOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#B37FEB' }} />
            );
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <FileDoneOutlined
                className={TYPE_ICON_CLASS_NAME}
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
            headerDropdownItems={headerDropdownItems}
            isNameEditable
            tabs={[
                {
                    id: EntityProfileTab.SUMMARY_TAB,
                    name: 'Summary',
                    component: DataProductSummaryTab,
                    icon: ReadOutlined,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                    icon: FileOutlined,
                },
                {
                    name: 'Assets',
                    getDynamicName: (entityData, _, loading) => {
                        const assetCount = entityData?.entities?.total;
                        return <TabNameWithCount name="Assets" count={assetCount} loading={loading} />;
                    },
                    component: DataProductEntitiesTab,
                    icon: AppstoreOutlined,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                    icon: UnorderedListOutlined,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    getSidebarSections = () => [
        {
            component: SidebarEntityHeader,
        },
        {
            component: SidebarAboutSection,
        },
        {
            component: SidebarNotesSection,
        },
        {
            component: SidebarOwnerSection,
        },
        {
            component: SidebarDomainSection,
            properties: {
                updateOnly: true,
            },
        },
        // TODO: Is someone actually using the below code?
        {
            component: SidebarViewDefinitionSection,
            display: {
                // to do - change when we have a GetDataProductQuery
                visible: (_, dataset: GetDatasetQuery) => (dataset?.dataset?.viewProperties?.logic && true) || false,
            },
        },
        {
            component: SidebarTagsSection,
        },
        {
            component: SidebarGlossaryTermsSection,
        },
        {
            component: SidebarStructuredProperties,
        },
        {
            component: StatusSection,
        },
    ];

    getSidebarTabs = () => [
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this asset',
            icon: UnorderedListOutlined,
        },
    ];

    renderPreview = (previewType: PreviewType, data: DataProduct, actions) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
                name={data.properties?.name || ''}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                globalTags={data.tags}
                glossaryTerms={data.glossaryTerms}
                domain={data.domain?.domain}
                entityCount={data?.entities?.total || undefined}
                externalUrl={data.properties?.externalUrl}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
                actions={actions}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataProduct;
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                data={genericProperties}
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
                headerDropdownItems={headerDropdownItems}
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
        const parentDomains = {
            domains: (data?.domain && [data?.domain?.domain]) || [],
            count: (data?.domain && 1) || 0,
        };
        return {
            name,
            externalUrl,
            entityCount,
            parentDomains,
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
