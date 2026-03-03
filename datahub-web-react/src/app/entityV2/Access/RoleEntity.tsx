import { IdentificationBadge } from '@phosphor-icons/react';
import * as React from 'react';
import styled from 'styled-components';

import RoleEntityProfile from '@app/entityV2/Access/RoleEntityProfile';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { urlEncodeUrn } from '@app/entityV2/shared/utils';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';

import { EntityType, Role, SearchResult } from '@types';

const PreviewRoleIcon = styled(IdentificationBadge).attrs({ weight: 'regular' })`
    font-size: 20px;
`;
// /**
//  * Definition of the DataHub Access Role entity.
//  */
export class RoleEntity implements Entity<Role> {
    type: EntityType = EntityType.Role;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <IdentificationBadge
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

    getAutoCompleteFieldName = () => 'name';

    getPathName: () => string = () => 'role';

    getCollectionName: () => string = () => 'Roles';

    getEntityName: () => string = () => 'Role';

    renderProfile: (urn: string) => JSX.Element = (_) => <RoleEntityProfile />;

    renderPreview = (previewType: PreviewType, data: Role) => {
        const genericProperties = this.getGenericEntityProperties(data);
        return (
            <DefaultPreviewCard
                data={genericProperties}
                description={data?.properties?.description || ''}
                name={this.displayName(data)}
                urn={data.urn}
                url={`/${this.getPathName()}/${urlEncodeUrn(data.urn)}`}
                logoComponent={<PreviewRoleIcon />}
                entityType={EntityType.Role}
                typeIcon={this.icon(14, IconStyleType.ACCENT)}
                previewType={previewType}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as Role);
    };

    displayName = (data: Role) => {
        return data.properties?.name || data.urn;
    };

    getOverridePropertiesFromEntity = (data: Role) => {
        return {
            name: data.properties?.name,
        };
    };

    getGenericEntityProperties = (role: Role) => {
        return getDataForEntityType({ data: role, entityType: this.type, getOverrideProperties: (data) => data });
    };

    supportedCapabilities = () => {
        return new Set([EntityCapabilityType.OWNERS]);
    };

    getGraphName = () => {
        return 'roleEntity';
    };
}
