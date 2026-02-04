import { QuestionOutlined } from '@ant-design/icons';
import * as React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity, IconStyleType } from '@app/entityV2/Entity';
import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';

import { EntityType, Restricted } from '@types';

import RestrictedIcon from '@images/restricted.svg';

/**
 * Definition of the DataHub Restricted entity for V2.
 * Used to represent entities that the current user does not have permission to view.
 */
export class RestrictedEntity implements Entity<Restricted> {
    type: EntityType = EntityType.Restricted;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <QuestionOutlined className={TYPE_ICON_CLASS_NAME} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return (
                <QuestionOutlined
                    className={TYPE_ICON_CLASS_NAME}
                    style={{ fontSize: fontSize || 'inherit', color: color || '#B37FEB' }}
                />
            );
        }

        return (
            <QuestionOutlined
                className={TYPE_ICON_CLASS_NAME}
                style={{ fontSize: fontSize || 'inherit', color: color || '#BFBFBF' }}
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

    renderEmbeddedProfile = (_: string) => <RestrictedEntityProfile />;

    renderPreview = () => {
        return <RestrictedEntityProfile />;
    };

    renderSearch = () => {
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

    getOverridePropertiesFromEntity = (_data?: Restricted | null): GenericEntityProperties => {
        return {
            name: 'Restricted Asset',
            exists: true,
        };
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

function RestrictedEntityProfile() {
    return (
        <div style={{ padding: '16px', textAlign: 'center' }}>
            <QuestionOutlined style={{ fontSize: 48, color: '#BFBFBF', marginBottom: 16 }} />
            <h3>Restricted Asset</h3>
            <p style={{ color: '#8C8C8C' }}>This asset is restricted. Please request access to see more.</p>
        </div>
    );
}
