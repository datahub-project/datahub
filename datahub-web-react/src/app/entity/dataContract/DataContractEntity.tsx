/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FileOutlined } from '@ant-design/icons';
import * as React from 'react';

import { Entity, IconStyleType } from '@app/entity/Entity';
import { getDataForEntityType } from '@app/entity/shared/containers/profile/utils';
import { TYPE_ICON_CLASS_NAME } from '@src/app/shared/constants';
import { DataContract, EntityType } from '@src/types.generated';

/**
 * Definition of the DataHub DataContract entity.
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

    isBrowseEnabled = () => false;

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
            getOverrideProperties: (newData) => newData,
        });
    };

    supportedCapabilities = () => {
        return new Set([]);
    };
}
