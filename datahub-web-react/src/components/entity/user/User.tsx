import * as React from 'react';
import { CorpUser, EntityType } from '../../../types.generated';
import { Entity, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import UserProfile from './UserProfile';

/**
 * Definition of the DataHub Dataset entity.
 */
export class UserEntity implements Entity<CorpUser> {
    type: EntityType = EntityType.CorpUser;

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    getAutoCompleteFieldName = () => 'username';

    getPathName: () => string = () => 'user';

    getCollectionName: () => string = () => 'Users';

    renderProfile: (urn: string) => JSX.Element = (_) => <UserProfile />;

    renderPreview = (_: PreviewType, data: CorpUser) => (
        <Preview
            urn={data.urn}
            name={data.info?.displayName || data.urn}
            title={data.info?.title || ''}
            photoUrl={data.editableInfo?.pictureLink || undefined}
        />
    );
}
