import TableIcon from '@src/images/table-icon.svg?react';
import * as React from 'react';
import styled from 'styled-components';
import { EntityType, SearchResult, StructuredPropertyEntity as StructuredProperty } from '../../../types.generated';
import DefaultPreviewCard from '../../previewV2/DefaultPreviewCard';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { urlEncodeUrn } from '../shared/utils';

const PreviewPropIcon = styled(TableIcon)`
    font-size: 20px;
`;

/**
 * Definition of the DataHub Structured Property entity.
 */
export class StructuredPropertyEntity implements Entity<StructuredProperty> {
    type: EntityType = EntityType.StructuredProperty;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TableIcon className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TableIcon className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <TableIcon
                className={TYPE_ICON_CLASS_NAME}
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'structuredProperty';

    getPathName: () => string = () => this.getGraphName();

    getCollectionName: () => string = () => 'Structured Properties';

    getEntityName: () => string = () => 'Structured Property';

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
                logoComponent={<PreviewPropIcon />}
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
