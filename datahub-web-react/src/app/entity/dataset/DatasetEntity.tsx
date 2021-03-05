import * as React from 'react';
import { DatabaseFilled, DatabaseOutlined } from '@ant-design/icons';
import { Dataset, EntityType } from '../../../types.generated';
import { Profile } from './profile/Profile';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';

/**
 * Definition of the DataHub Dataset entity.
 */
export class DatasetEntity implements Entity<Dataset> {
    type: EntityType = EntityType.Dataset;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DatabaseOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DatabaseFilled style={{ fontSize, color: '#B37FEB' }} />;
        }

        return (
            <DatabaseFilled
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'dataset';

    getCollectionName = () => 'Datasets';

    renderProfile = (urn: string) => <Profile urn={urn} />;

    renderPreview = (_: PreviewType, data: Dataset) => {
        return (
            <Preview
                urn={data.urn}
                name={data.name}
                origin={data.origin}
                description={data.description}
                platformName={data.platform.name}
                platformLogo={data.platform.info?.logoUrl}
                tags={data.tags}
                owners={data.ownership?.owners}
            />
        );
    };
}
