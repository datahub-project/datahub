import { TagFilled, TagOutlined } from '@ant-design/icons';
import * as React from 'react';
import styled from 'styled-components';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entity/Entity';
import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import { urlEncodeUrn } from '@app/entity/shared/utils';
import TagProfile from '@app/entity/tag/TagProfile';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';

import { useGetTagQuery } from '@graphql/tag.generated';
import { EntityType, SearchResult, Tag } from '@types';

const PreviewTagIcon = styled(TagOutlined)`
    font-size: 20px;
`;

/**
 * Definition of the DataHub Tag entity.
 */
export class TagEntity implements Entity<Tag> {
    type: EntityType = EntityType.Tag;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TagFilled style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TagFilled style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <TagOutlined
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

    getGraphName = () => 'tag';

    getPathName: () => string = () => 'tag';

    getCollectionName: () => string = () => 'Tags';

    getEntityName: () => string = () => 'Tag';

    useEntityQuery = useGetTagQuery;

    renderProfile: (urn: string) => JSX.Element = (_) => <TagProfile />;

    renderPreview = (_: PreviewType, data: Tag) => (
        <DefaultPreviewCard
            description={data.description || ''}
            name={this.displayName(data)}
            urn={data.urn}
            url={`/${this.getPathName()}/${urlEncodeUrn(data.urn)}`}
            logoComponent={<PreviewTagIcon />}
            type="Tag"
            typeIcon={this.icon(14, IconStyleType.ACCENT)}
        />
    );

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
