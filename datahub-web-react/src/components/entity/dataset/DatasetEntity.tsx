import * as React from 'react';
import { Dataset, EntityType } from '../../../types.generated';
import { Profile } from './profile/Profile';
import { Entity, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';

/**
 * Definition of the DataHub Dataset entity.
 */
export class DatasetEntity implements Entity<Dataset> {
    type: EntityType = EntityType.Dataset;

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'dataset';

    getCollectionName = () => 'Datasets';

    renderProfile = (urn: string) => <Profile urn={urn} />;

    renderPreview = (_: PreviewType, data: any) => {
        return (
            <Preview
                urn={data.urn}
                name={data.name}
                origin={data.origin}
                description={data.description}
                platformNativeType={data.platformNativeType}
            />
        );
    };
}
