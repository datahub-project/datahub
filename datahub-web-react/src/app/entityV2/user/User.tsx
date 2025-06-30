import { UserOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import UserProfile from '@app/entityV2/user/UserProfile';
import { Preview } from '@app/entityV2/user/preview/Preview';

import { CorpUser, EntityType, SearchResult } from '@types';

/**
 * Definition of the DataHub Dataset entity.
 */
export class UserEntity implements Entity<CorpUser> {
    type: EntityType = EntityType.CorpUser;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <UserOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <UserOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        return (
            <UserOutlined
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

    getAutoCompleteFieldName = () => 'username';

    getGraphName: () => string = () => 'corpuser';

    getPathName: () => string = () => 'user';

    getEntityName = () => 'Person';

    getCollectionName: () => string = () => 'People';

    renderProfile = (urn: string) => <UserProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: CorpUser) => (
        <Preview
            urn={data.urn}
            name={this.displayName(data)}
            title={data.editableProperties?.title || data.info?.title || ''}
        />
    );

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as CorpUser);
    };

    displayName = (data: CorpUser) => {
        return (
            data.editableProperties?.displayName ||
            data.properties?.displayName ||
            data.properties?.fullName ||
            data.info?.displayName || // Deprecated info field
            data.info?.fullName || // Deprecated info field
            data.username ||
            data.urn
        );
    };

    getGenericEntityProperties = (user: CorpUser) => {
        return getDataForEntityType({ data: user, entityType: this.type, getOverrideProperties: (data) => data });
    };

    supportedCapabilities = () => {
        return new Set([EntityCapabilityType.ROLES]);
    };
}
