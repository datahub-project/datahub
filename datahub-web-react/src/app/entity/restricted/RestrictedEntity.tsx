import React from 'react';
import { QuestionOutlined } from '@ant-design/icons';
import { EntityType, Restricted, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import RestrictedIcon from '../../../images/restricted.svg';
import { RestrictedEntityProfile } from './RestrictedEntityProfile';

/**
 * Definition of the DataHub Data Product entity.
 */
export class RestrictedEntity implements Entity<Restricted> {
    type: EntityType = EntityType.Restricted;

    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <QuestionOutlined />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <QuestionOutlined style={{ fontSize, color: color || '#B37FEB' }} />;
        }

        return (
            <QuestionOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'restricted';

    getEntityName = () => 'Restricted';

    getCollectionName = () => 'Restricted Assets';

    renderProfile = (_: string) => <RestrictedEntityProfile />;

    renderPreview = (_: PreviewType, _data: Restricted) => {
        return <RestrictedEntityProfile />;
    };

    renderSearch = (_result: SearchResult) => {
        return <RestrictedEntityProfile />;
    };

    getLineageVizConfig = (entity: Restricted) => {
        return {
            urn: entity?.urn,
            name: 'Restricted Asset',
            type: EntityType.Restricted,
            icon: RestrictedIcon,
        };
    };

    displayName = (_data: Restricted) => {
        return 'Restricted Asset';
    };

    getOverridePropertiesFromEntity = (_data: Restricted) => {
        return {};
    };

    getGenericEntityProperties = (data: Restricted) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: this.getOverridePropertiesFromEntity,
        });
    };

    supportedCapabilities = () => {
        return new Set([]);
    };

    getGraphName = () => {
        return 'restricted';
    };
}
