import { TeamOutlined } from '@ant-design/icons';
import * as React from 'react';
import { CorpGroup, EntityType, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import GroupProfile from './GroupProfile';
import { getDataForEntityType } from '../shared/containers/profile/utils';

/**
 * Definition of the DataHub CorpGroup entity.
 */
export class GroupEntity implements Entity<CorpGroup> {
    type: EntityType = EntityType.CorpGroup;

    // TODO: update icons for UserGroup
    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TeamOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TeamOutlined style={{ fontSize }} />;
        }

        return (
            <TeamOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName: () => string = () => 'group';

    getEntityName = () => 'Group';

    getCollectionName: () => string = () => 'Groups';

    renderProfile: (urn: string) => JSX.Element = (_) => <GroupProfile />;

    renderPreview = (_: PreviewType, data: CorpGroup) => (
        <Preview
            urn={data.urn}
            name={this.displayName(data)}
            description={data.info?.description}
            membersCount={(data as any)?.memberCount?.total || 0}
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
