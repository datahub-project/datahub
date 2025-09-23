import * as React from 'react';

import DomainIcon from '@app/domain/DomainIcon';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entity/Entity';
import DataProductsTab from '@app/entity/domain/DataProductsTab/DataProductsTab';
import { DomainEntitiesTab } from '@app/entity/domain/DomainEntitiesTab';
import { Preview } from '@app/entity/domain/preview/Preview';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { EntityProfileTab } from '@app/entity/shared/constants';
import { EntityProfile } from '@app/entity/shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '@app/entity/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '@app/entity/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarStructuredPropsSection from '@app/entity/shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';
import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import { EntityActionItem } from '@app/entity/shared/entity/EntityActions';
import { DocumentationTab } from '@app/entity/shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '@app/entity/shared/tabs/Properties/PropertiesTab';

import { useGetDomainQuery } from '@graphql/domain.generated';
import { Domain, EntityType, SearchResult } from '@types';

/**
 * Definition of the DataHub Domain entity.
 */
export class DomainEntity implements Entity<Domain> {
    type: EntityType = EntityType.Domain;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DomainIcon />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DomainIcon style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path
                    fillRule="evenodd"
                    clipRule="evenodd"
                    d="M5.38241 4.45017C5.79578 5.08156 6.64272 5.2583 7.2741 4.84493C7.90549 4.43156 8.08223 3.58462 7.66886 2.95323C7.25549 2.32184 6.40855 2.14511 5.77716 2.55847C5.14578 2.97184 4.96904 3.81878 5.38241 4.45017ZM5.14394 5.70522C5.93376 6.24949 7.0063 6.2952 7.85837 5.73735C8.54707 5.28646 8.93198 4.54586 8.95765 3.78126C10.4383 3.8373 11.732 4.62372 12.4888 5.79063C12.8376 5.61918 13.2124 5.53858 13.5832 5.54251C12.6507 3.85455 10.8528 2.71139 8.78798 2.71139L8.74856 2.71152C8.76299 2.74381 8.77666 2.7763 8.78957 2.80897C8.72977 2.65776 8.65383 2.51033 8.56128 2.36896C7.82523 1.24471 6.31715 0.930005 5.1929 1.66606C4.11266 2.37329 3.77982 3.79327 4.40835 4.90069C3.72087 5.81574 3.31348 6.95324 3.31348 8.18589C3.31348 9.17754 3.57714 10.1076 4.03819 10.9098C4.2114 10.6603 4.43456 10.4392 4.70432 10.2626C4.75298 10.2307 4.80235 10.2008 4.85235 10.1729C4.55031 9.57585 4.38014 8.90074 4.38014 8.18589C4.38014 7.26597 4.66195 6.41188 5.14394 5.70522ZM12.4163 8.72427C12.8297 9.35566 13.6767 9.5324 14.308 9.11903C14.9394 8.70566 15.1162 7.85872 14.7028 7.22733C14.2894 6.59594 13.4425 6.4192 12.8111 6.83257C12.1797 7.24594 12.003 8.09288 12.4163 8.72427ZM13.7973 10.3978C14.1761 10.3609 14.5518 10.2344 14.8923 10.0114C16.0166 9.27539 16.3313 7.76732 15.5952 6.64306C14.8592 5.51881 13.3511 5.2041 12.2268 5.94015C11.1026 6.6762 10.7879 8.18428 11.5239 9.30854C11.8145 9.75234 12.2254 10.07 12.6854 10.2467C11.9457 11.6427 10.4779 12.5937 8.78798 12.5937C8.67553 12.5937 8.56407 12.5895 8.45373 12.5812C8.51807 12.0342 8.39801 11.4624 8.07271 10.9655C7.33667 9.84128 5.82859 9.52658 4.70433 10.2626C3.58008 10.9987 3.26537 12.5068 4.00142 13.631C4.73747 14.7553 6.24555 15.07 7.36981 14.3339C7.66496 14.1407 7.90432 13.8942 8.08351 13.6155C8.31415 13.6451 8.54928 13.6604 8.78798 13.6604C11.0243 13.6604 12.9475 12.3195 13.7973 10.3978ZM6.78554 13.4415C6.15415 13.8549 5.30721 13.6781 4.89384 13.0467C4.48047 12.4154 4.65721 11.5684 5.2886 11.155C5.91999 10.7417 6.76693 10.9184 7.1803 11.5498C7.59367 12.1812 7.41693 13.0281 6.78554 13.4415Z"
                    fill="currentColor"
                />
            );
        }

        return (
            <DomainIcon
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'domain';

    getPathName = () => 'domain';

    getEntityName = () => 'Domain';

    getCollectionName = () => 'Domains';

    useEntityQuery = useGetDomainQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.Domain}
            useEntityQuery={useGetDomainQuery}
            useUpdateQuery={undefined}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={new Set([EntityMenuItems.MOVE, EntityMenuItems.DELETE])}
            headerActionItems={new Set([EntityActionItem.BATCH_ADD_DOMAIN])}
            isNameEditable
            tabs={[
                {
                    id: EntityProfileTab.DOMAIN_ENTITIES_TAB,
                    name: 'Entities',
                    component: DomainEntitiesTab,
                },
                {
                    id: EntityProfileTab.DOCUMENTATION_TAB,
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    id: EntityProfileTab.DATA_PRODUCTS_TAB,
                    name: 'Data Products',
                    component: DataProductsTab,
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
        },
        {
            component: SidebarStructuredPropsSection,
        },
    ];

    renderPreview = (_: PreviewType, data: Domain) => {
        return (
            <Preview
                domain={data}
                urn={data.urn}
                name={this.displayName(data)}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Domain;
        return (
            <Preview
                domain={data}
                urn={data.urn}
                name={this.displayName(data)}
                description={data.properties?.description}
                owners={data.ownership?.owners}
                logoComponent={this.icon(12, IconStyleType.ACCENT)}
            />
        );
    };

    displayName = (data: Domain) => {
        return data?.properties?.name || data?.id || data.urn;
    };

    getOverridePropertiesFromEntity = (data: Domain) => {
        return {
            name: data.properties?.name,
        };
    };

    getGenericEntityProperties = (data: Domain) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    supportedCapabilities = () => {
        // TODO.. Determine whether SOFT_DELETE should go into here.
        return new Set([EntityCapabilityType.OWNERS]);
    };
}
