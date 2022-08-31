import { UserOutlined } from '@ant-design/icons';
import * as React from 'react';
import { CorpUser, EntityType, SearchResult } from '../../../types.generated';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { Preview } from './preview/Preview';
import UserProfile from './UserProfile';

/**
 * Definition of the DataHub Dataset entity.
 */
export class UserEntity implements Entity<CorpUser> {
    type: EntityType = EntityType.CorpUser;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <UserOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <UserOutlined style={{ fontSize }} />;
        }

        return (
            <UserOutlined
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

    getAutoCompleteFieldName = () => 'username';

    getPathName: () => string = () => 'user';

    getEntityName = () => 'Person';

    getCollectionName: () => string = () => 'People';

    renderProfile: (urn: string) => JSX.Element = (_) => <UserProfile />;

    renderPreview = (_: PreviewType, data: CorpUser) => (
        <Preview
            urn={data.urn}
            name={this.displayName(data)}
            title={data.editableProperties?.title || data.info?.title || ''}
            photoUrl={data.editableInfo?.pictureLink || undefined}
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
