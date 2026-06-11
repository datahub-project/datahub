import i18next from 'i18next';

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
    get [SummaryElementType.Created]() {
        return i18next.t('common.labels:created');
    },
    get [SummaryElementType.LastModified]() {
        return i18next.t('entity.profile.summary:properties.lastModified');
    },
    // intentional UX rename: "Last Synced" is clearer to end users than "Last Ingested"
    get [SummaryElementType.LastIngested]() {
        return i18next.t('entity.profile.summary:properties.lastSynced');
    },
    get [SummaryElementType.Domain]() {
        return i18next.t('common.labels:domain');
    },
    get [SummaryElementType.GlossaryTerms]() {
        return i18next.t('entity.profile.summary:properties.glossaryTerms');
    },
    get [SummaryElementType.Owners]() {
        return i18next.t('common.labels:owners');
    },
    get [SummaryElementType.Tags]() {
        return i18next.t('common.labels:tags');
    },
    get [SummaryElementType.DocumentStatus]() {
        return i18next.t('common.labels:status');
    },
    get [SummaryElementType.DocumentType]() {
        return i18next.t('common.labels:type');
    },
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
