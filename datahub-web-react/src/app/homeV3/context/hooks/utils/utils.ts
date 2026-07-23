import {
    AI_CONTEXT_MODULE,
    ASSETS_MODULE,
    CHILD_HIERARCHY_MODULE,
    COLUMNS_MODULE,
    DATA_PRODUCTS_MODULE,
    LINEAGE_MODULE,
    OUTPUT_PORTS_MODULE,
    RELATED_METRICS_MODULE,
    RELATED_TERMS_MODULE,
    SEMANTIC_MODEL_DATASETS_MODULE,
    SEMANTIC_MODEL_DIMENSIONS_MODULE,
    SEMANTIC_MODEL_METRICS_MODULE,
    SEMANTIC_MODEL_RELATIONSHIPS_MODULE,
    SQL_MODULE,
} from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';
import { EntityType, PageTemplateScope, PageTemplateSurfaceType, SummaryElement, SummaryElementType } from '@types';

const CREATED = { elementType: SummaryElementType.Created };
const LAST_MODIFIED = { elementType: SummaryElementType.LastModified };
const LAST_INGESTED = { elementType: SummaryElementType.LastIngested };
const OWNERS = { elementType: SummaryElementType.Owners };
const DOMAIN = { elementType: SummaryElementType.Domain };
const TAGS = { elementType: SummaryElementType.Tags };
const GLOSSARY_TERMS = { elementType: SummaryElementType.GlossaryTerms };
const DOCUMENT_STATUS = { elementType: SummaryElementType.DocumentStatus };
const DOCUMENT_TYPE = { elementType: SummaryElementType.DocumentType };
const SEMANTIC_MODEL = { elementType: SummaryElementType.SemanticModel };

export function getDefaultSummaryPageTemplate(entityType: EntityType): PageTemplateFragment {
    let rows: { modules: PageModuleFragment[] }[] = [{ modules: [] }];
    let summaryElements: SummaryElement[] = [];

    switch (entityType) {
        case EntityType.Domain:
            rows = [{ modules: [ASSETS_MODULE, CHILD_HIERARCHY_MODULE] }, { modules: [DATA_PRODUCTS_MODULE] }];
            summaryElements = [CREATED, OWNERS];
            break;
        case EntityType.DataProduct:
            rows = [{ modules: [OUTPUT_PORTS_MODULE, ASSETS_MODULE] }];
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
            rows = [{ modules: [LINEAGE_MODULE] }, { modules: [COLUMNS_MODULE] }];
            summaryElements = [CREATED, OWNERS, DOMAIN, TAGS, GLOSSARY_TERMS];
            break;
        case EntityType.SemanticModel:
            rows = [
                { modules: [LINEAGE_MODULE] },
                { modules: [SEMANTIC_MODEL_DATASETS_MODULE, SEMANTIC_MODEL_METRICS_MODULE] },
                { modules: [SEMANTIC_MODEL_RELATIONSHIPS_MODULE, SEMANTIC_MODEL_DIMENSIONS_MODULE] },
                { modules: [AI_CONTEXT_MODULE] },
            ];
            summaryElements = [LAST_INGESTED, DOMAIN, OWNERS, GLOSSARY_TERMS];
            break;
        case EntityType.Metric:
            rows = [
                { modules: [LINEAGE_MODULE] },
                { modules: [SQL_MODULE, RELATED_METRICS_MODULE] },
                { modules: [AI_CONTEXT_MODULE] },
            ];
            summaryElements = [CREATED, LAST_MODIFIED, OWNERS, SEMANTIC_MODEL];
            break;
        case EntityType.Document:
            rows = [{ modules: [] }];
            summaryElements = [DOCUMENT_TYPE, DOCUMENT_STATUS, CREATED, LAST_MODIFIED, LAST_INGESTED, OWNERS];
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
