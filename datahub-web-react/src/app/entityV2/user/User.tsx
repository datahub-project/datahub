import { User } from '@phosphor-icons/react';
import * as React from 'react';

import { Entity, EntityCapabilityType, IconStyleType, PreviewContext, PreviewType } from '@app/entityV2/Entity';
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
        return (
            <User
                className={TYPE_ICON_CLASS_NAME}
                size={fontSize || 14}
                color={color || 'currentColor'}
                weight={styleType === IconStyleType.HIGHLIGHT ? 'fill' : 'regular'}
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

    renderPreview = (_: PreviewType, data: CorpUser, _actions, extraContext?: PreviewContext) => (
        <Preview
            urn={data.urn}
            name={this.displayName(data)}
            title={data.editableProperties?.title || data.info?.title || ''}
            propagationDetails={extraContext?.propagationDetails}
        />
    );

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as CorpUser, undefined, undefined);
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
