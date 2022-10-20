import { TagOutlined, TagFilled } from '@ant-design/icons';
import * as React from 'react';
import styled from 'styled-components';
import { Tag, EntityType, SearchResult } from '../../../types.generated';
import DefaultPreviewCard from '../../preview/DefaultPreviewCard';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { urlEncodeUrn } from '../shared/utils';
import TagProfile from './TagProfile';

const PreviewTagIcon = styled(TagOutlined)`
    font-size: 20px;
`;

/**
 * Definition of the DataHub Tag entity.
 */
export class TagEntity implements Entity<Tag> {
    type: EntityType = EntityType.Tag;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TagFilled style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TagFilled style={{ fontSize, color: '#B37FEB' }} />;
        }

        return (
            <TagOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName: () => string = () => 'tag';

    getCollectionName: () => string = () => 'Tags';

    getEntityName: () => string = () => 'Tag';

    renderProfile: (urn: string) => JSX.Element = (_) => <TagProfile />;

    renderPreview = (_: PreviewType, data: Tag) => (
        <DefaultPreviewCard
            description={data.description || ''}
            name={data.name}
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
