import React from 'react';

import { Entity, EntityMenuActions, IconStyleType, PreviewType } from '@app/entityV2/Entity';

import { EntityType, SearchResult } from '@types';

class DefaultEntity implements Entity<null> {
    type: EntityType = EntityType.Other;

    icon = (_fontSize?: number | undefined, _styleType?: IconStyleType | undefined, _color?: string | undefined) => (
        <></>
    );

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getPathName = () => '';

    getCollectionName = () => '';

    renderProfile = (_urn: string) => <></>;

    renderPreview = (_type: PreviewType, _data: null, _actions?: EntityMenuActions | undefined) => <></>;

    renderSearch = (_result: SearchResult) => <></>;

    displayName = (_data: null) => '';

    getGenericEntityProperties = (_data: null) => null;

    supportedCapabilities = () => new Set([]);

    getGraphName = () => '';
}

export default new DefaultEntity();
