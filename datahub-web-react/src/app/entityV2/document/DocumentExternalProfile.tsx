import React from 'react';

import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import SummaryTab from '@app/entityV2/summary/SummaryTab';

import { useGetDocumentQuery } from '@graphql/document.generated';
import { Document, EntityType } from '@types';

const headerDropdownItems = new Set([EntityMenuItems.COPY_URL, EntityMenuItems.SHARE]);

/**
 * Profile for external documents (documents ingested from external sources like Slack, Notion, etc.)
 * Uses the traditional EntityProfile with tabs instead of the custom DocumentSimpleProfile
 */
export const DocumentExternalProfile = ({ urn }: { urn: string }): JSX.Element => {
    const getOverrideProperties = (document: Document) => {
        return {
            name: document.info?.title,
        };
    };

    return (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Document}
            useEntityQuery={useGetDocumentQuery}
            getOverrideProperties={getOverrideProperties}
            headerDropdownItems={headerDropdownItems}
            tabs={[
                {
                    name: 'Summary',
                    component: SummaryTab,
                    display: {
                        visible: () => true,
                        enabled: () => true,
                    },
                },
                {
                    name: 'Properties',
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
            ]}
        />
    );
};
