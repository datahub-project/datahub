import * as React from 'react';
import { ShareAltOutlined } from '@ant-design/icons';
import { MlFeatureTable, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { MLFeatureTableProfile } from './profile/MLFeatureTableProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';

/**
 * Definition of the DataHub MLFeatureTable entity.
 */
export class MLFeatureTableEntity implements Entity<MlFeatureTable> {
    type: EntityType = EntityType.MlfeatureTable;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <ShareAltOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <ShareAltOutlined style={{ fontSize, color: '#d6246c' }} />;
        }

        return (
            <ShareAltOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'mlFeatureTable';

    getCollectionName = () => 'MLFeatureTable';

    renderProfile = (urn: string) => <MLFeatureTableProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: MlFeatureTable) => {
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                description={data.description}
                owners={data.ownership?.owners}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as MlFeatureTable;
        return (
            <Preview
                urn={data.urn}
                name={data.name || ''}
                description={data.description || ''}
                owners={data.ownership?.owners}
            />
        );
    };
}
