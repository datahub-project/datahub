import { FileOutlined } from '@ant-design/icons';
import { TYPE_ICON_CLASS_NAME } from '@src/app/shared/constants';
import { DataContract, EntityType } from '@src/types.generated';
import * as React from 'react';
import { Entity, IconStyleType } from '../Entity';
import { getDataForEntityType } from '../shared/containers/profile/utils';

/**
 * Definition of the DataHub DataFlow entity.
 */
export class DataContractEntity implements Entity<DataContract> {
    type: EntityType = EntityType.DataContract;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <FileOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <FileOutlined className={TYPE_ICON_CLASS_NAME} style={{ fontSize, color: color || '#d6246c' }} />;
        }

        return (
            <FileOutlined
                className={TYPE_ICON_CLASS_NAME}
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getGraphName = () => 'dataContract';

    getPathName = () => 'dataContracts';

    getEntityName = () => 'Data Contract';

    getCollectionName = () => 'Data Contracts';

    renderProfile = () => <span>Not Implemented</span>;

    getSidebarSections = () => [];

    getSidebarTabs = () => [];

    getOverridePropertiesFromEntity = () => {};

    renderPreview = () => {
        return <span>Not Implemented</span>;
    };

    renderSearch = () => {
        return <span>Not Implemented</span>;
    };

    displayName = () => {
        return 'Data Contract';
    };

    getGenericEntityProperties = (data: DataContract) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
        });
    };

    supportedCapabilities = () => {
        return new Set([]);
    };
}
