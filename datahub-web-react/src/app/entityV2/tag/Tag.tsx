import { Tag as TagIcon } from '@phosphor-icons/react';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewContext, PreviewType } from '@app/entityV2/Entity';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { urlEncodeUrn } from '@app/entityV2/shared/utils';
import TagProfile from '@app/entityV2/tag/TagProfile';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';

import { EntityType, SearchResult, Tag } from '@types';

/**
 * Definition of the DataHub Tag entity.
 */
export class TagEntity implements Entity<Tag> {
    type: EntityType = EntityType.Tag;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <TagIcon
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'tag';

    getPathName: () => string = () => this.getGraphName();

    getCollectionName: () => string = () => 'Tags';

    getEntityName: () => string = () => 'Tag';

    renderProfile: (urn: string) => JSX.Element = (urn) => <TagProfile urn={urn} />;

    renderPreview = (previewType: PreviewType, data: Tag, _actions, extraContext?: PreviewContext) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <DefaultPreviewCard
                data={genericProperties}
                description={data.description || ''}
                name={this.displayName(data)}
                urn={data.urn}
                url={`/${this.getPathName()}/${urlEncodeUrn(data.urn)}`}
                logoComponent={<TagIcon size={20} color="currentColor" />}
                entityType={EntityType.Tag}
                typeIcon={this.icon(14, IconStyleType.ACCENT)}
                previewType={previewType}
                propagationDetails={extraContext?.propagationDetails}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as Tag, undefined, undefined);
    };

    displayName = (data: Tag) => {
        return data.properties?.name || data.name || data.urn;
    };

    getGenericEntityProperties = (tag: Tag) => {
        return getDataForEntityType({ data: tag, entityType: this.type, getOverrideProperties: (data) => data });
    };

    supportedCapabilities = () => {
        return new Set([EntityCapabilityType.OWNERS]);
    };
}
