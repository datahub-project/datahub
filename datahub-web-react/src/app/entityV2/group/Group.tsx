import { TeamOutlined } from '@ant-design/icons';
import * as React from 'react';
import { CorpGroup, EntityType, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import GroupProfile from './GroupProfile';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';

/**
 * Definition of the DataHub CorpGroup entity.
 */
export class GroupEntity implements Entity<CorpGroup> {
    type: EntityType = EntityType.CorpGroup;

    // TODO: update icons for UserGroup
    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TeamOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TeamOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        return (
            <TeamOutlined
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

    getGraphName: () => string = () => 'corpGroup';

    getPathName: () => string = () => 'group';

    getEntityName = () => 'Group';

    getCollectionName: () => string = () => 'Groups';

    renderProfile = (urn: string) => <GroupProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: CorpGroup) => (
        <Preview
            urn={data.urn}
            name={this.displayName(data)}
            description={data.info?.description}
            membersCount={(data as any)?.memberCount?.total || (data as any)?.relationships?.total || 0}
        />
    );

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as CorpGroup);
    };

    displayName = (data: CorpGroup) => {
        return data.properties?.displayName || data.info?.displayName || data.name || data.urn;
    };

    getGenericEntityProperties = (group: CorpGroup) => {
        return getDataForEntityType({ data: group, entityType: this.type, getOverrideProperties: (data) => data });
    };

    supportedCapabilities = () => {
        return new Set([]);
    };
}
