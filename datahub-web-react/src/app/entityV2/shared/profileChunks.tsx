import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';

// One React.lazy per module. Entity files import these wrappers, so a chart page
// reuses the dataset page's already-resolved profile chunk instead of suspending again.

export const AccessManagement = lazyProfileComponent(
    'AccessManagement',
    () => import('@app/entityV2/shared/tabs/Dataset/AccessManagement/AccessManagement'),
);

export const DAGTab = lazyProfileComponent('DAGTab', () =>
    import('@app/entityV2/shared/tabs/Lineage/DAGTab').then((module) => ({
        default: module.DAGTab,
    })),
);

export const DataProductSection = lazyProfileComponent(
    'DataProductSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/DataProduct/DataProductSection'),
);

export const DocumentationTab = lazyProfileComponent('DocumentationTab', () =>
    import('@app/entityV2/shared/tabs/Documentation/DocumentationTab').then((module) => ({
        default: module.DocumentationTab,
    })),
);

export const EmbedTab = lazyProfileComponent('EmbedTab', () =>
    import('@app/entityV2/shared/tabs/Embed/EmbedTab').then((module) => ({
        default: module.EmbedTab,
    })),
);

export const EmbeddedProfile = lazyProfileComponent(
    'EmbeddedProfile',
    () => import('@app/entityV2/shared/embed/EmbeddedProfile'),
);

export const EntityProfile = lazyProfileComponent('EntityProfile', () =>
    import('@app/entityV2/shared/containers/profile/EntityProfile').then((module) => ({
        default: module.EntityProfile,
    })),
);

export const IncidentTab = lazyProfileComponent('IncidentTab', () =>
    import('@app/entityV2/shared/tabs/Incident/IncidentTab').then((module) => ({
        default: module.IncidentTab,
    })),
);

export const LineageTab = lazyProfileComponent('LineageTab', () =>
    import('@app/entityV2/shared/tabs/Lineage/LineageTab').then((module) => ({
        default: module.LineageTab,
    })),
);

export const PropertiesTab = lazyProfileComponent('PropertiesTab', () =>
    import('@app/entityV2/shared/tabs/Properties/PropertiesTab').then((module) => ({
        default: module.PropertiesTab,
    })),
);

export const RunsTab = lazyProfileComponent('RunsTab', () =>
    import('@app/entityV2/dataJob/tabs/RunsTab').then((module) => ({
        default: module.RunsTab,
    })),
);

export const SchemaTab = lazyProfileComponent('SchemaTab', () =>
    import('@app/entityV2/shared/tabs/Dataset/Schema/SchemaTab').then((module) => ({
        default: module.SchemaTab,
    })),
);

export const SidebarAboutSection = lazyProfileComponent('SidebarAboutSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection').then((module) => ({
        default: module.SidebarAboutSection,
    })),
);

export const SidebarApplicationSection = lazyProfileComponent('SidebarApplicationSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/Applications/SidebarApplicationSection').then((module) => ({
        default: module.SidebarApplicationSection,
    })),
);

export const SidebarDomainSection = lazyProfileComponent('SidebarDomainSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/Domain/SidebarDomainSection').then((module) => ({
        default: module.SidebarDomainSection,
    })),
);

export const SidebarEntityHeader = lazyProfileComponent(
    'SidebarEntityHeader',
    () => import('@app/entityV2/shared/containers/profile/sidebar/SidebarEntityHeader'),
);

export const SidebarGlossaryTermsSection = lazyProfileComponent('SidebarGlossaryTermsSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/SidebarGlossaryTermsSection').then((module) => ({
        default: module.SidebarGlossaryTermsSection,
    })),
);

export const SidebarLineageSection = lazyProfileComponent(
    'SidebarLineageSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageSection'),
);

export const SidebarNotesSection = lazyProfileComponent(
    'SidebarNotesSection',
    () => import('@app/entityV2/shared/sidebarSection/SidebarNotesSection'),
);

export const SidebarOwnerSection = lazyProfileComponent('SidebarOwnerSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection').then((module) => ({
        default: module.SidebarOwnerSection,
    })),
);

export const SidebarQueryOperationsSection = lazyProfileComponent(
    'SidebarQueryOperationsSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/Query/SidebarQueryOperationsSection'),
);

export const SidebarStructuredProperties = lazyProfileComponent(
    'SidebarStructuredProperties',
    () => import('@app/entityV2/shared/sidebarSection/SidebarStructuredProperties'),
);

export const SidebarTagsSection = lazyProfileComponent('SidebarTagsSection', () =>
    import('@app/entityV2/shared/containers/profile/sidebar/SidebarTagsSection').then((module) => ({
        default: module.SidebarTagsSection,
    })),
);

export const StatusSection = lazyProfileComponent(
    'StatusSection',
    () => import('@app/entityV2/shared/containers/profile/sidebar/shared/StatusSection'),
);

export const SummaryTab = lazyProfileComponent('SummaryTab', () => import('@app/entityV2/summary/SummaryTab'));
