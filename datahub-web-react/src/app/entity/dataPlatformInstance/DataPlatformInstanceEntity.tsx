import * as React from 'react';
import { DataPlatformInstance, EntityType } from '../../../types.generated';
import { Entity } from '../Entity';
import { GenericEntityProperties } from '../shared/types';
import { getDataForEntityType } from '../shared/containers/profile/utils';

/**
 * Definition of the DataHub DataPlatformInstance entity.
 * Most of this still needs to be filled out.
 */
export class DataPlatformInstanceEntity implements Entity<DataPlatformInstance> {
    type: EntityType = EntityType.DataPlatformInstance;

    icon = () => {
        return <></>;
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'dataPlatformInstance';

    getEntityName = () => 'Data Platform Instance';

    getCollectionName = () => 'Data Platform Instances';

    renderProfile = () => <></>;

    getOverridePropertiesFromEntity = (): GenericEntityProperties => {
        return {};
    };

    renderPreview = () => {
        return <></>;
    };

    renderSearch = () => {
        return <></>;
    };

    displayName = (data: DataPlatformInstance) => {
        return data?.instanceId || data.urn;
    };

    getGenericEntityProperties = (data: DataPlatformInstance) => {
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
        return 'dataPlatformInstance';
    };
}
