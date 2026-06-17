import { ListDashes } from '@phosphor-icons/react/dist/csr/ListDashes';
import i18next from 'i18next';
import * as React from 'react';

import { Entity, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { urlEncodeUrn } from '@app/entityV2/shared/utils';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';

import { EntityType, SearchResult, StructuredPropertyEntity as StructuredProperty } from '@types';

/**
 * Definition of the DataHub Structured Property entity.
 */
export class StructuredPropertyEntity implements Entity<StructuredProperty> {
    type: EntityType = EntityType.StructuredProperty;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <ListDashes
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'structuredProperty';

    getPathName: () => string = () => this.getGraphName();

    getCollectionName: () => string = () => i18next.t('entity.types:structuredProperty.namePlural');

    getEntityName: () => string = () => i18next.t('entity.types:structuredProperty.name');

    renderProfile: (urn: string) => JSX.Element = (_urn) => <div />; // not used right now

    renderPreview = (previewType: PreviewType, data: StructuredProperty) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <DefaultPreviewCard
                data={genericProperties}
                description={data.definition?.description || ''}
                name={this.displayName(data)}
                urn={data.urn}
                url={`/${this.getPathName()}/${urlEncodeUrn(data.urn)}`}
                logoComponent={<ListDashes size={20} color="currentColor" />}
                entityType={EntityType.StructuredProperty}
                typeIcon={this.icon(14, IconStyleType.ACCENT)}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as StructuredProperty);
    };

    displayName = (data: StructuredProperty) => {
        return data?.definition?.displayName || data?.definition?.qualifiedName || data?.urn;
    };

    getGenericEntityProperties = (entity: StructuredProperty) => {
        return getDataForEntityType({ data: entity, entityType: this.type, getOverrideProperties: (data) => data });
    };

    supportedCapabilities = () => {
        return new Set([]);
    };
}
