import React from 'react';
import { useTranslation } from 'react-i18next';

import { DataObjectEntity } from '@app/entityV2/dataObject/DataObjectEntity';
import { EntityProfile } from '@app/entityV2/shared/containers/profile/EntityProfile';
import { SidebarApplicationSection } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection';
import DataProductSection from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarGlossaryTermsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection';
import SidebarStructuredProperties from '@app/entityV2/shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';

import { useGetDataObjectQuery } from '@graphql/dataObject.generated';
import { EntityType } from '@types';

export const DataObjectProfile = ({ urn }: { urn: string }): JSX.Element => {
    const { t } = useTranslation('entity.types');
    return (
        <EntityProfile
            urn={urn}
            entityType={EntityType.DataObject}
            useEntityQuery={useGetDataObjectQuery}
            getOverrideProperties={new DataObjectEntity().getOverridePropertiesFromEntity}
            tabs={[
                {
                    name: t('tab.documentation'),
                    component: DocumentationTab,
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
                    component: SidebarApplicationSection,
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
                    component: SidebarStructuredProperties,
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
