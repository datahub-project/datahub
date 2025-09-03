import { ASSETS_MODULE, CHILD_HIERARCHY_MODULE } from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';
import { EntityType, PageTemplateScope, PageTemplateSurfaceType, SummaryElement, SummaryElementType } from '@types';

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
            modules = [ASSETS_MODULE, CHILD_HIERARCHY_MODULE];
            summaryElements = [CREATED, OWNERS];
            break;
        case EntityType.DataProduct:
            modules = [ASSETS_MODULE];
            summaryElements = [CREATED, OWNERS, DOMAIN, TAGS, GLOSSARY_TERMS];
            break;
        case EntityType.GlossaryTerm:
            modules = [ASSETS_MODULE];
            summaryElements = [CREATED, OWNERS, DOMAIN];
            break;
        case EntityType.GlossaryNode:
            modules = [CHILD_HIERARCHY_MODULE];
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
