import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import { BookOpen } from '@phosphor-icons/react/dist/csr/BookOpen';
import { File } from '@phosphor-icons/react/dist/csr/File';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { SquaresFour } from '@phosphor-icons/react/dist/csr/SquaresFour';
import i18next from 'i18next';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/application/preview/Preview';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { EntityProfileTab } from '@app/entityV2/shared/constants';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/entityData';
import { EntityActionItem } from '@app/entityV2/shared/entity/EntityActions';
import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import {
    DocumentationTab,
    EntityProfile,
    PropertiesTab,
    SidebarAboutSection,
    SidebarDomainSection,
    SidebarEntityHeader,
    SidebarGlossaryTermsSection,
    SidebarNotesSection,
    SidebarOwnerSection,
    SidebarStructuredProperties,
    SidebarTagsSection,
    StatusSection,
    SummaryTab,
} from '@app/entityV2/shared/profileChunks';
import { EntityTab } from '@app/entityV2/shared/types';
import { useShowAssetSummaryPage } from '@app/entityV2/summary/useShowAssetSummaryPage';

import { useGetApplicationQuery } from '@graphql/application.generated';
import { Application, EntityType, SearchResult } from '@types';

const ApplicationSummaryTab = lazyProfileComponent('ApplicationSummaryTab', () =>
    import('@app/entityV2/application/ApplicationSummaryTab').then((module) => ({
        default: module.ApplicationSummaryTab,
    })),
);

const ApplicationEntitiesTab = lazyProfileComponent('ApplicationEntitiesTab', () =>
    import('@app/entityV2/application/ApplicationEntitiesTab').then((module) => ({
        default: module.ApplicationEntitiesTab,
    })),
);

const headerDropdownItems = new Set([EntityMenuItems.SHARE, EntityMenuItems.DELETE, EntityMenuItems.EDIT]);

type ApplicationWithChildren = Application & {
    children?: {
        total: number;
    } | null;
};

/**
 * Definition of the DataHub Application entity.
 */
export class ApplicationEntity implements Entity<Application> {
    type: EntityType = EntityType.Application;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <AppWindow
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'application';

    getEntityName = () => i18next.t('entity.types:application.name');

    getCollectionName = () => i18next.t('entity.types:application.namePlural');

    useEntityQuery = useGetApplicationQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Application}
            useEntityQuery={useGetApplicationQuery}
            useUpdateQuery={undefined}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerActionItems={new Set([EntityActionItem.BATCH_ADD_APPLICATION])}
            headerDropdownItems={headerDropdownItems}
            isNameEditable
            tabs={this.getProfileTabs()}
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
            name: i18next.t('entity.types:tab.properties'),
            component: PropertiesTab,
            description: i18next.t('entity.types:sidebar.propertiesDescription'),
            icon: ListBullets,
        },
    ];

    getProfileTabs = (): EntityTab[] => {
        const showSummaryTab = useShowAssetSummaryPage();

        return [
            {
                id: EntityProfileTab.SUMMARY_TAB,
                name: i18next.t('entity.types:tab.summary'),
                component: showSummaryTab ? SummaryTab : ApplicationSummaryTab,
                icon: BookOpen,
            },
            ...(!showSummaryTab
                ? [
                      {
                          name: i18next.t('entity.types:tab.documentation'),
                          component: DocumentationTab,
                          icon: File,
                      },
                  ]
                : []),
            {
                name: i18next.t('entity.types:tab.assets'),
                getCount: (entityData, _, loading) => {
                    return !loading ? entityData?.children?.total : undefined;
                },
                component: ApplicationEntitiesTab,
                icon: SquaresFour,
            },
            {
                name: i18next.t('entity.types:tab.properties'),
                component: PropertiesTab,
                icon: ListBullets,
            },
        ];
    };

    renderPreview = (previewType: PreviewType, data: Application, actions) => {
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
                parentApplications={data.parentApplications?.applications}
                entityCount={(data as ApplicationWithChildren)?.children?.total || undefined}
                externalUrl={data.properties?.externalUrl}
                headerDropdownItems={headerDropdownItems}
                previewType={previewType}
                actions={actions}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Application;
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
                parentApplications={data.parentApplications?.applications}
                entityCount={(data as ApplicationWithChildren)?.children?.total || undefined}
                externalUrl={data.properties?.externalUrl}
                degree={(result as any).degree}
                paths={(result as any).paths}
                headerDropdownItems={headerDropdownItems}
                previewType={PreviewType.SEARCH}
            />
        );
    };

    displayName = (data: Application) => {
        return data?.properties?.name || data.urn;
    };

    getOverridePropertiesFromEntity = (data: Application) => {
        const name = data?.properties?.name;
        const externalUrl = data?.properties?.externalUrl;
        const entityCount = (data as ApplicationWithChildren)?.children?.total || undefined;
        return {
            name,
            externalUrl,
            entityCount,
        };
    };

    getGenericEntityProperties = (data: Application) => {
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
            EntityCapabilityType.RELATED_DOCUMENTS,
            EntityCapabilityType.FORMS,
        ]);
    };

    getGraphName = () => {
        return 'application';
    };
}
