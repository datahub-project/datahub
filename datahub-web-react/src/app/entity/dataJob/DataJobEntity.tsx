import * as React from 'react';
import { DatabaseFilled, DatabaseOutlined } from '@ant-design/icons';
import { DataJob, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { DataJobProfile } from './profile/DataJobProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';

/**
 * Definition of the DataHub DataJob entity.
 */
export class DataJobEntity implements Entity<DataJob> {
    type: EntityType = EntityType.DataJob;

    // TODO: add job specific icons
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

    getPathName = () => 'datajob';

    getCollectionName = () => 'DataJobs';

    renderProfile = (urn: string) => <DataJobProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: DataJob) => {
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.info?.description || ''}
                owners={data.ownership?.owners}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataJob;
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.info?.description || ''}
                owners={data.ownership?.owners}
            />
        );
    };
}
