import { AppstoreOutlined, FileOutlined, LayoutOutlined, UnorderedListOutlined } from '@ant-design/icons';
import { BookmarkSimple } from '@phosphor-icons/react';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewContext, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/glossaryTerm/preview/Preview';
import GlossaryRelatedEntity from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedEntity';
import GlossayRelatedTerms from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTerms';
import { RelatedTermTypes } from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTermsResult';
import useGlossaryRelatedAssetsTabCount from '@app/entityV2/glossaryTerm/profile/useGlossaryRelatedAssetsTabCount';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarApplicationSection } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { EntityActionItem } from '@app/entityV2/shared/entity/EntityActions';
import SidebarNotesSection from '@app/entityV2/shared/sidebarSection/SidebarNotesSection';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { SchemaTab } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTab';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import { EntityTab } from '@app/entityV2/shared/types';
import SummaryTab from '@app/entityV2/summary/SummaryTab';
import { useShowAssetSummaryPage } from '@app/entityV2/summary/useShowAssetSummaryPage';
import { FetchedEntity } from '@app/lineage/types';

import { GetGlossaryTermQuery, useGetGlossaryTermQuery } from '@graphql/glossaryTerm.generated';
import { EntityType, GlossaryTerm, SearchResult } from '@types';

const headerDropdownItems = new Set([
    EntityMenuItems.MOVE,
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.CLONE,
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
        return (
            <BookmarkSimple
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
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
                tabs={this.getProfileTabs()}
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
            component: SidebarApplicationSection,
        },
        {
            component: SidebarStructuredProperties,
        },
        {
            component: StatusSection,
        },
    ];

    getProfileTabs = (): EntityTab[] => {
        const showSummaryTab = useShowAssetSummaryPage();

        return [
            ...(showSummaryTab
                ? [
                      {
                          name: 'Summary',
                          component: SummaryTab,
                          id: 'asset-summary-tab',
                      },
                  ]
                : []),
            ...(!showSummaryTab
                ? [
                      {
                          name: 'Documentation',
                          component: DocumentationTab,
                          icon: FileOutlined,
                      },
                  ]
                : []),
            {
                name: 'Related Assets',
                getCount: useGlossaryRelatedAssetsTabCount,
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
                getCount: (entityData, _, loading) => {
                    const totalRelatedTerms = Object.keys(RelatedTermTypes).reduce((acc, curr) => {
                        return acc + (entityData?.[curr]?.total || 0);
                    }, 0);
                    return !loading ? totalRelatedTerms : undefined;
                },
                component: GlossayRelatedTerms,
                icon: () => <BookmarkSimple style={{ marginRight: 6 }} />,
            },
            {
                name: 'Properties',
                component: PropertiesTab,
                icon: UnorderedListOutlined,
            },
        ];
    };

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
        return this.renderPreview(PreviewType.SEARCH, result.entity as GlossaryTerm, undefined, undefined);
    };

    renderPreview = (previewType: PreviewType, data: GlossaryTerm, _actions, extraContext?: PreviewContext) => {
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
                propagationDetails={extraContext?.propagationDetails}
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
            EntityCapabilityType.APPLICATIONS,
            EntityCapabilityType.RELATED_DOCUMENTS,
            EntityCapabilityType.FORMS,
        ]);
    };

    getGraphName = () => this.getPathName();
}
