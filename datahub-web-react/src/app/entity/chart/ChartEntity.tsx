import { LineChartOutlined } from '@ant-design/icons';
import * as React from 'react';
import { Chart, EntityType } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { ChartPreview } from './preview/ChartPreview';
import ChartProfile from './profile/ChartProfile';

/**
 * Definition of the DataHub Chart entity.
 */
export class ChartEntity implements Entity<Chart> {
    type: EntityType = EntityType.Chart;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <LineChartOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <LineChartOutlined style={{ fontSize, color: 'rgb(144 163 236)' }} />;
        }

        return (
            <LineChartOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

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
                access={data.info?.access}
                owners={data.ownership?.owners}
                tags={data?.globalTags || undefined}
            />
        );
    };
}
