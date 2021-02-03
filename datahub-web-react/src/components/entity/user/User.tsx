import * as React from 'react';
import { CorpUser, EntityType } from '../../../types.generated';
import { Entity, PreviewType } from '../Entity';
import { UserPage } from './UserPage';

/**
 * Definition of the DataHub Dataset entity.
 */
export class UserEntity implements Entity<CorpUser> {
    type: EntityType = EntityType.User;

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    getAutoCompleteFieldName = () => 'username';

    getPathName: () => string = () => 'user';

    getCollectionName: () => string = () => 'Users';

    renderProfile: (urn: string) => JSX.Element = (_) => <UserPage />;

    renderPreview = (_: PreviewType, _1: CorpUser) => <p>Hello</p>;
}
