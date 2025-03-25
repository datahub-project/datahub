import { CodeSandboxOutlined, UnorderedListOutlined } from '@ant-design/icons';
import * as React from 'react';
import { useGetMlModelDeploymentQuery } from '../../../graphql/mlModelDeployment.generated';
import { EntityType, MlModelDeployment, SearchResult } from '../../../types.generated';
import { GenericEntityProperties } from '../../entity/shared/types';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { EntityMenuItems } from '../shared/EntityDropdown/EntityMenuActions';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { SidebarAboutSection } from '../shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '../shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '../shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import SidebarEntityHeader from '../shared/containers/profile/sidebar/SidebarEntityHeader';
import { SidebarGlossaryTermsSection } from '../shared/containers/profile/sidebar/SidebarGlossaryTermsSection';
import { SidebarTagsSection } from '../shared/containers/profile/sidebar/SidebarTagsSection';
import StatusSection from '../shared/containers/profile/sidebar/shared/StatusSection';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import SidebarStructuredProperties from '../shared/sidebarSection/SidebarStructuredProperties';
import { DocumentationTab } from '../shared/tabs/Documentation/DocumentationTab';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import MlModelDeploymentSummary from './profile/MLModelDeploymentSummary';
import Preview from './preview/Preview';
import SidebarNotesSection from '../shared/sidebarSection/SidebarNotesSection';

const headerDropdownItems = new Set([
    EntityMenuItems.SHARE,
    EntityMenuItems.UPDATE_DEPRECATION,
    EntityMenuItems.ANNOUNCE,
    EntityMenuItems.EXTERNAL_URL,
]);

/**
 * Definition of the DataHub MlModelDeployment entity.
 */
export class MLModelDeploymentEntity implements Entity<MlModelDeployment> {
    type: EntityType = EntityType.MlmodelGroup;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <CodeSandboxOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <CodeSandboxOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#9633b9' }} />
            );
        }

        return (
            <CodeSandboxOutlined
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

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'mlModelDeployment';

    getPathName = () => 'mlModelDeployment';

    getEntityName = () => 'ML Model Deployment';

    getCollectionName = () => 'ML Model Deployments';

    getOverridePropertiesFromEntity = (mlModelDeployment?: MlModelDeployment | null): GenericEntityProperties => {
        return {
            name: mlModelDeployment && this.displayName(mlModelDeployment),
        };
    };

    useEntityQuery = useGetMlModelDeploymentQuery;

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            key={urn}
            entityType={EntityType.MlmodelDeployment}
            useEntityQuery={useGetMlModelDeploymentQuery}
            getOverrideProperties={this.getOverridePropertiesFromEntity}
            headerDropdownItems={headerDropdownItems}
            tabs={[
                {
                    name: 'Summary',
                    component: MlModelDeploymentSummary,
                },
                {
                    name: 'Documentation',
                    component: DocumentationTab,
                },
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
            ]}
            sidebarSections={this.getSidebarSections()}
            sidebarTabs={this.getSidebarTabs()}
        />
    );

    renderPreview = (_: PreviewType, data: MlModelDeployment) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                data={genericProperties}            
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlModelDeployment
        const genericProperties = this.getGenericEntityProperties(data);

        return (
            <Preview
                urn={data.urn}
                name={this.displayName(data)}
                data={genericProperties}       
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
        },
        {
            component: DataProductSection,
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


    displayName = (data: MlModelDeployment) => {
        // eslint-disable-next-line @typescript-eslint/dot-notation
        return data.properties?.['propertiesName'] || data.properties?.name || data.name || data.urn;
    };

    getGenericEntityProperties = (mlModelDeployment: MlModelDeployment) => {
        return getDataForEntityType({
            data: mlModelDeployment,
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
            EntityCapabilityType.DEPRECATION,
            EntityCapabilityType.SOFT_DELETE,
            EntityCapabilityType.DATA_PRODUCTS,
            EntityCapabilityType.LINEAGE,
        ]);
    };
}
