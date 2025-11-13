import {
    ASSETS_MODULE,
    CHILD_HIERARCHY_MODULE,
    DATA_PRODUCTS_MODULE,
    RELATED_TERMS_MODULE,
} from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';
import { EntityType, PageTemplateScope, PageTemplateSurfaceType, SummaryElement, SummaryElementType } from '@types';

const CREATED = { elementType: SummaryElementType.Created };
const LAST_MODIFIED = { elementType: SummaryElementType.LastModified };
const OWNERS = { elementType: SummaryElementType.Owners };
const DOMAIN = { elementType: SummaryElementType.Domain };
const TAGS = { elementType: SummaryElementType.Tags };
const GLOSSARY_TERMS = { elementType: SummaryElementType.GlossaryTerms };
const DOCUMENT_STATUS = { elementType: SummaryElementType.DocumentStatus };
const DOCUMENT_TYPE = { elementType: SummaryElementType.DocumentType };

export function getDefaultSummaryPageTemplate(entityType: EntityType): PageTemplateFragment {
    let rows: { modules: PageModuleFragment[] }[] = [{ modules: [] }];
    let summaryElements: SummaryElement[] = [];

    switch (entityType) {
        case EntityType.Domain:
            rows = [{ modules: [ASSETS_MODULE, CHILD_HIERARCHY_MODULE] }, { modules: [DATA_PRODUCTS_MODULE] }];
            summaryElements = [CREATED, OWNERS];
            break;
        case EntityType.DataProduct:
            rows = [{ modules: [ASSETS_MODULE] }];
            summaryElements = [CREATED, OWNERS, DOMAIN, TAGS, GLOSSARY_TERMS];
            break;
        case EntityType.GlossaryTerm:
            rows = [{ modules: [ASSETS_MODULE, RELATED_TERMS_MODULE] }];
            summaryElements = [CREATED, OWNERS, DOMAIN];
            break;
        case EntityType.GlossaryNode:
            rows = [{ modules: [CHILD_HIERARCHY_MODULE] }];
            summaryElements = [CREATED, OWNERS];
            break;
        case EntityType.Dataset:
            summaryElements = [CREATED, OWNERS, DOMAIN, TAGS, GLOSSARY_TERMS];
        case EntityType.Document:
            rows = [{ modules: [] }];
            summaryElements = [DOCUMENT_TYPE, DOCUMENT_STATUS, CREATED, LAST_MODIFIED, OWNERS];
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
            rows,
            assetSummary: { summaryElements },
        },
    };
}

export function filterNonExistentStructuredProperties(summaryElements) {
    return summaryElements.filter((element) =>
        element.elementType === SummaryElementType.StructuredProperty ? element.structuredProperty?.exists : true,
    );
}
