import React from 'react';
import { useTranslation } from 'react-i18next';

import { DocumentEntity } from '@app/entityV2/document/DocumentEntity';
import ExternalDocumentInlineSummaryTab from '@app/entityV2/document/ExternalDocumentInlineSummaryTab';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarRelatedAssetsSection } from '@app/entityV2/shared/containers/profile/sidebar/RelatedAssets/SidebarRelatedAssetsSection';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';

import { useGetDocumentQuery } from '@graphql/document.generated';
import { EntityType } from '@types';

const headerDropdownItems = new Set([EntityMenuItems.COPY_URL, EntityMenuItems.SHARE]);

/**
 * Profile for external documents (documents ingested from external sources like Slack, Notion, etc.)
 * Uses the traditional EntityProfile with tabs instead of the custom DocumentSimpleProfile
 */
export const DocumentExternalProfile = ({ urn }: { urn: string }): JSX.Element => {
    const { t } = useTranslation('entity.types');
    return (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Document}
            useEntityQuery={useGetDocumentQuery}
            getOverrideProperties={new DocumentEntity().getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            tabs={[
                {
                    name: t('tab.summary'),
                    component: ExternalDocumentInlineSummaryTab,
                    display: {
                        visible: () => true,
                        enabled: () => true,
                    },
                },
                {
                    name: t('tab.properties'),
                    component: PropertiesTab,
                    display: {
                        visible: () => true,
                        enabled: () => true,
                    },
                },
            ]}
            sidebarSections={[
                {
                    component: SidebarOwnerSection,
                    display: {
                        visible: () => true,
                    },
                },
                {
                    component: SidebarTagsSection,
                    display: {
                        visible: () => true,
                    },
                },
                {
                    component: SidebarGlossaryTermsSection,
                    display: {
                        visible: () => true,
                    },
                },
                {
                    component: SidebarDomainSection,
                    display: {
                        visible: () => true,
                    },
                },
                {
                    component: DataProductSection,
                    display: {
                        visible: () => true,
                    },
                },
                {
                    component: SidebarRelatedAssetsSection,
                    display: {
                        visible: () => true,
                    },
                },
                {
                    component: StatusSection,
                    display: {
                        visible: (entityData) => !!entityData?.lastIngested,
                    },
                },
            ]}
        />
    );
};
