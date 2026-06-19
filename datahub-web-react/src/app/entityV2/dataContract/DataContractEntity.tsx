import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import i18next from 'i18next';
import * as React from 'react';

import { Entity, IconStyleType } from '@app/entityV2/Entity';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { TYPE_ICON_CLASS_NAME } from '@src/app/shared/constants';
import { DataContract, EntityType } from '@src/types.generated';

/**
 * Definition of the DataHub DataContract entity.
 */
export class DataContractEntity implements Entity<DataContract> {
    type: EntityType = EntityType.DataContract;

    icon = (fontSize?: number, styleType?: IconStyleType, color?: string) => {
        return (
            <FileText
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

    getGraphName = () => 'dataContract';

    getPathName = () => 'dataContracts';

    getEntityName = () => i18next.t('entity.types:dataContract.name');

    getCollectionName = () => i18next.t('entity.types:dataContract.namePlural');

    renderProfile = () => <span>{i18next.t('entity.types:dataContract.notImplemented')}</span>;

    getSidebarSections = () => [];

    getSidebarTabs = () => [];

    getOverridePropertiesFromEntity = () => {};

    renderPreview = () => {
        return <span>{i18next.t('entity.types:dataContract.notImplemented')}</span>;
    };

    renderSearch = () => {
        return <span>{i18next.t('entity.types:dataContract.notImplemented')}</span>;
    };

    displayName = () => {
        return i18next.t('entity.types:dataContract.name');
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
