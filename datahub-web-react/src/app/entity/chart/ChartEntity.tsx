import * as React from 'react';
import { Chart, EntityType } from '../../../types.generated';
import { Entity, PreviewType } from '../Entity';
import { ChartPreview } from './preview/ChartPreview';
import ChartProfile from './profile/ChartProfile';

/**
 * Definition of the DataHub Chart entity.
 */
export class ChartEntity implements Entity<Chart> {
    type: EntityType = EntityType.Chart;

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    getAutoCompleteFieldName = () => 'title';

    getPathName = () => 'chart';

    getCollectionName = () => 'Charts';

    renderProfile = (urn: string) => <ChartProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: Chart) => {
        return (
            <ChartPreview
                urn={data.urn}
                platform={data.tool}
                name={data.info?.name}
                description={data.info?.description}
            />
        );
    };
}
