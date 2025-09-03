import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';
import {
    DataHubPageModuleType,
    EntityType,
    PageModuleScope,
    PageTemplateScope,
    PageTemplateSurfaceType,
    SummaryElement,
    SummaryElementType,
} from '@types';

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

const asssetsModule: PageModuleFragment = {
    urn: 'urn:li:dataHubPageModule:assets',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Assets',
        type: DataHubPageModuleType.Assets,
        visibility: {
            scope: PageModuleScope.Global,
        },
        params: {},
    },
};

const CREATED = { elementType: SummaryElementType.Created };
const OWNERS = { elementType: SummaryElementType.Owners };
const DOMAIN = { elementType: SummaryElementType.Domain };
const TAGS = { elementType: SummaryElementType.Tags };
const GLOSSARY_TERMS = { elementType: SummaryElementType.GlossaryTerms };

// TODO: apply actual functionality once the required modules exist
export function getDefaultSummaryPageTemplate(entityType: EntityType): PageTemplateFragment {
    let modules: PageModuleFragment[] = [];
    let summaryElements: SummaryElement[] = [];

    switch (entityType) {
        case EntityType.Domain:
            modules = [domainsModule, asssetsModule];
            summaryElements = [CREATED, OWNERS];
            break;
        case EntityType.DataProduct:
            modules = [domainsModule, asssetsModule];
            summaryElements = [CREATED, OWNERS, DOMAIN, TAGS, GLOSSARY_TERMS];
            break;
        case EntityType.GlossaryTerm:
            modules = [domainsModule, asssetsModule];
            summaryElements = [CREATED, OWNERS, DOMAIN];
            break;
        case EntityType.GlossaryNode:
            modules = [domainsModule];
            summaryElements = [CREATED, OWNERS];
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
            assetSummary: { summaryElements },
        },
    };
}
