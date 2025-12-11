/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { TagFilled, TagOutlined } from '@ant-design/icons';
import * as React from 'react';
import styled from 'styled-components';

import RoleEntityProfile from '@app/entityV2/Access/RoleEntityProfile';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { urlEncodeUrn } from '@app/entityV2/shared/utils';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';

import { EntityType, Role, SearchResult } from '@types';

const PreviewTagIcon = styled(TagOutlined)`
    font-size: 20px;
`;
// /**
//  * Definition of the DataHub Access Role entity.
//  */
export class RoleEntity implements Entity<Role> {
    type: EntityType = EntityType.Role;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TagFilled className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TagFilled className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <TagOutlined
                className={TYPE_ICON_CLASS_NAME}
                style={{ fontSize: fontSize || 'inherit', color: color || 'inherit' }}
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
                logoComponent={<PreviewTagIcon />}
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
