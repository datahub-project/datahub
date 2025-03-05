import { AppstoreOutlined, FileOutlined, LayoutOutlined, UnorderedListOutlined } from '@ant-design/icons';
import * as React from 'react';
import { BookmarkSimple } from '@phosphor-icons/react';
import { GetGlossaryTermQuery, useGetGlossaryTermQuery } from '../../../graphql/glossaryTerm.generated';
import { EntityType, GlossaryTerm, SearchResult } from '../../../types.generated';
import { GenericEntityProperties } from '../../entity/shared/types';
import { FetchedEntity } from '../../lineage/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '../shared/containers/profile/sidebar/SidebarEntityHeader';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { EntityActionItem } from '../shared/entity/EntityActions';
import SidebarStructuredProperties from '../shared/sidebarSection/SidebarStructuredProperties';
import { SchemaTab } from '../shared/tabs/Dataset/Schema/SchemaTab';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import TabNameWithCount from '../shared/tabs/Entity/TabNameWithCount';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import GlossaryRelatedAssetsTabHeader from './GlossaryRelatedAssetsTabHeader';
import { Preview } from './preview/Preview';
import GlossaryRelatedEntity from './profile/GlossaryRelatedEntity';
import GlossayRelatedTerms from './profile/GlossaryRelatedTerms';
import { RelatedTermTypes } from './profile/GlossaryRelatedTermsResult';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';

const headerDropdownItems = new Set([
    EntityMenuItems.MOVE,
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.DELETE,
    EntityMenuItems.ANNOUNCE,
]);

/**
 * Definition of the DataHub Dataset entity.
 */
export class GlossaryTermEntity implements Entity<GlossaryTerm> {
    getLineageVizConfig?: ((entity: GlossaryTerm) => FetchedEntity) | undefined;

    type: EntityType = EntityType.GlossaryTerm;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <BookmarkSimple className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <BookmarkSimple
                    className={TYPE_ICON_CLASS_NAME}
                    style={{ fontSize, color: color || '#B37FEB' }}
                    weight="fill"
                />
            );
        }

        if (styleType === IconStyleType.ACCENT) {
            return <BookmarkSimple style={{ fontSize: fontSize ?? 10, color: color || '#6C6B88' }} />;
        }

        return (
            <BookmarkSimple
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

    getAutoCompleteFieldName = () => 'name';

    isLineageEnabled = () => false;

    getPathName = () => 'glossaryTerm';

    getCollectionName = () => 'Glossary Terms';

    getEntityName = () => 'Glossary Term';

    useEntityQuery = useGetGlossaryTermQuery;

    renderProfile = (urn) => {
        return (
            <EntityProfile
                urn={urn}
                entityType={EntityType.GlossaryTerm}
                useEntityQuery={useGetGlossaryTermQuery as any}
                headerActionItems={new Set([EntityActionItem.BATCH_ADD_GLOSSARY_TERM])}
                headerDropdownItems={headerDropdownItems}
                isNameEditable
                tabs={[
                    {
                        name: 'Documentation',
                        component: DocumentationTab,
                        icon: FileOutlined,
                    },
                    {
                        name: 'Related Assets',
                        getDynamicName: GlossaryRelatedAssetsTabHeader,
                        component: GlossaryRelatedEntity,
                        icon: AppstoreOutlined,
                    },
                    {
                        name: 'Schema',
                        component: SchemaTab,
                        icon: LayoutOutlined,
                        properties: {
                            editMode: false,
                        },
                        display: {
                            visible: (_, glossaryTerm: GetGlossaryTermQuery) =>
                                glossaryTerm?.glossaryTerm?.schemaMetadata !== null,
                            enabled: (_, glossaryTerm: GetGlossaryTermQuery) =>
                                glossaryTerm?.glossaryTerm?.schemaMetadata !== null,
                        },
                    },
                    {
                        name: 'Related Terms',
                        getDynamicName: (entityData, _, loading) => {
                            const totalRelatedTerms = Object.keys(RelatedTermTypes).reduce((acc, curr) => {
                                return acc + (entityData?.[curr]?.total || 0);
                            }, 0);
                            return (
                                <TabNameWithCount name="Related Terms" count={totalRelatedTerms} loading={loading} />
                            );
                        },
                        component: GlossayRelatedTerms,
                        icon: () => <BookmarkSimple style={{ marginRight: 6 }} />,
                    },
                    {
                        name: 'Properties',
                        component: PropertiesTab,
                        icon: UnorderedListOutlined,
                    },
                ]}
                sidebarSections={this.getSidebarSections()}
                getOverrideProperties={this.getOverridePropertiesFromEntity}
                sidebarTabs={this.getSidebarTabs()}
            />
        );
    };

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
                hideOwnerType: true,
            },
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

    getOverridePropertiesFromEntity = (glossaryTerm?: GlossaryTerm | null): GenericEntityProperties => {
        // if dataset has subTypes filled out, pick the most specific subtype and return it
        return {
            customProperties: glossaryTerm?.properties?.customProperties,
        };
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as GlossaryTerm);
    };

    renderPreview = (previewType: PreviewType, data: GlossaryTerm) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                data={genericProperties}
                previewType={previewType}
                urn={data?.urn}
                parentNodes={data.parentNodes}
                name={this.displayName(data)}
                description={data?.properties?.description || ''}
                owners={data?.ownership?.owners}
                deprecation={data?.deprecation}
                domain={data.domain?.domain}
                headerDropdownItems={headerDropdownItems}
            />
        );
    };

    displayName = (data: GlossaryTerm) => {
        return data?.properties?.name || data?.name || data?.urn;
    };

    platformLogoUrl = (_: GlossaryTerm) => {
        return undefined;
    };

    getGenericEntityProperties = (glossaryTerm: GlossaryTerm) => {
        return getDataForEntityType({
            data: glossaryTerm,
            entityType: this.type,
            getOverrideProperties: (data) => data,
        });
    };

    supportedCapabilities = () => {
        return new Set([
            EntityCapabilityType.OWNERS,
            EntityCapabilityType.DEPRECATION,
            EntityCapabilityType.SOFT_DELETE,
        ]);
    };

    getGraphName = () => this.getPathName();
}
