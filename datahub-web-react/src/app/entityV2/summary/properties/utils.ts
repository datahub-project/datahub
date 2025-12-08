import { MenuItemType } from '@components/components/Menu/types';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { AssetProperty } from '@app/entityV2/summary/properties/types';

import { SummaryElementFragment } from '@graphql/template.generated';
import { SummaryElementType } from '@types';

export function assetPropertyToMenuItem(
    assetProperty: AssetProperty,
    onMenuItemClick: (assetProperty: AssetProperty) => void,
): MenuItemType {
    return {
        type: 'item',
        key: assetProperty.key ?? assetProperty.type,
        title: assetProperty.name,
        icon: assetProperty.icon,
        onClick: () => onMenuItemClick(assetProperty),
    };
}

const SUMMARY_ELEMENT_TYPE_TO_NAME = {
    [SummaryElementType.Created]: 'Created',
    [SummaryElementType.LastModified]: 'Last Modified',
    [SummaryElementType.Domain]: 'Domain',
    [SummaryElementType.GlossaryTerms]: 'Glossary Terms',
    [SummaryElementType.Owners]: 'Owners',
    [SummaryElementType.Tags]: 'Tags',
    [SummaryElementType.DocumentStatus]: 'Status',
    [SummaryElementType.DocumentType]: 'Type',
};

export function mapSummaryElement(
    summaryElement: SummaryElementFragment,
    entityRegistry: EntityRegistry,
): AssetProperty {
    const { structuredProperty } = summaryElement;
    return {
        name:
            summaryElement.elementType === SummaryElementType.StructuredProperty && structuredProperty
                ? entityRegistry.getDisplayName(structuredProperty.type, structuredProperty)
                : SUMMARY_ELEMENT_TYPE_TO_NAME[summaryElement.elementType] || summaryElement.elementType,
        type: summaryElement.elementType,
        structuredProperty: structuredProperty || undefined,
    };
}
