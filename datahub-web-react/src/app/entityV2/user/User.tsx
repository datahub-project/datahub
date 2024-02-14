import { UserOutlined } from '@ant-design/icons';
import * as React from 'react';
import { CorpUser, EntityType, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { Preview } from './preview/Preview';
import UserProfile from './UserProfile';
import { TYPE_ICON_CLASS_NAME } from '../shared/components/subtypes';

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
