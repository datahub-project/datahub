import { TagFilled, TagOutlined } from '@ant-design/icons';
import * as React from 'react';
import styled from 'styled-components';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { urlEncodeUrn } from '@app/entityV2/shared/utils';
import TagProfile from '@app/entityV2/tag/TagProfile';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';

import { EntityType, SearchResult, Tag } from '@types';

const PreviewTagIcon = styled(TagOutlined)`
    font-size: 20px;
`;

/**
 * Definition of the DataHub Tag entity.
 */
export class TagEntity implements Entity<Tag> {
    type: EntityType = EntityType.Tag;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TagFilled className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TagFilled className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <TagOutlined
                className={TYPE_ICON_CLASS_NAME}
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
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

    renderPreview = (previewType: PreviewType, data: Tag) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <DefaultPreviewCard
                data={genericProperties}
                description={data.description || ''}
                name={this.displayName(data)}
                urn={data.urn}
                url={`/${this.getPathName()}/${urlEncodeUrn(data.urn)}`}
                logoComponent={<PreviewTagIcon />}
                entityType={EntityType.Tag}
                typeIcon={this.icon(14, IconStyleType.ACCENT)}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as Tag);
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
