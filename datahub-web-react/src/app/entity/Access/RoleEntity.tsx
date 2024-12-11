import { TagOutlined, TagFilled } from '@ant-design/icons';
import * as React from 'react';
import styled from 'styled-components';
import { Role, EntityType, SearchResult } from '../../../types.generated';
import DefaultPreviewCard from '../../preview/DefaultPreviewCard';
import { Entity, EntityCapabilityType, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { urlEncodeUrn } from '../shared/utils';
import RoleEntityProfile from './RoleEntityProfile';
import { useGetExternalRoleQuery } from '../../../graphql/accessrole.generated';

const PreviewTagIcon = styled(TagOutlined)`
    font-size: 20px;
`;
// /**
//  * Definition of the DataHub Access Role entity.
//  */
export class RoleEntity implements Entity<Role> {
    type: EntityType = EntityType.Role;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TagFilled style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TagFilled style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <TagOutlined
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

    getAutoCompleteFieldName = () => 'name';

    getPathName: () => string = () => 'role';

    getCollectionName: () => string = () => 'Roles';

    getEntityName: () => string = () => 'Role';

    useEntityQuery = useGetExternalRoleQuery;

    renderProfile: (urn: string) => JSX.Element = (_) => <RoleEntityProfile />;

    renderPreview = (_: PreviewType, data: Role) => (
        <DefaultPreviewCard
            description={data?.properties?.description || ''}
            name={this.displayName(data)}
            urn={data.urn}
            url={`/${this.getPathName()}/${urlEncodeUrn(data.urn)}`}
            logoComponent={<PreviewTagIcon />}
            type="Role"
            typeIcon={this.icon(14, IconStyleType.ACCENT)}
        />
    );

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
