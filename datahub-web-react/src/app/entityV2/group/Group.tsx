/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { TeamOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Entity, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import GroupProfile from '@app/entityV2/group/GroupProfile';
import { Preview } from '@app/entityV2/group/preview/Preview';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';

import { CorpGroup, EntityType, SearchResult } from '@types';

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
                style={{ fontSize: fontSize || 'inherit', color: color || 'inherit' }}
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
