import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

const domainsModule: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:top_domains',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Domains',
        type: DataHubPageModuleType.Domains,
        visibility: {
            scope: PageModuleScope.Global,
        },
        params: {},
    },
};

// TODO: apply actual functionality once the required modules exist
export function getDefaultSummaryPageTemplate(entityType: EntityType) {
    let modules: PageModuleFragment[] = [];

    switch (entityType) {
        case EntityType.Domain:
            modules = [domainsModule];
            break;
        case EntityType.DataProduct:
            modules = [domainsModule];
            break;
        case EntityType.GlossaryTerm:
            modules = [domainsModule];
            break;
        case EntityType.GlossaryNode:
            modules = [domainsModule];
            break;
        default:
            break;
    }

    return {
        urn: 'urn:li:dataHubPageTemplate:asset_summary_default',
        type: EntityType.DatahubPageTemplate,
        properties: {
            visibility: {
                scope: PageTemplateScope.Personal,
            },
            surface: {
                surfaceType: PageTemplateSurfaceType.AssetSummary,
            },
            rows: [{ modules }],
        },
    };
}
