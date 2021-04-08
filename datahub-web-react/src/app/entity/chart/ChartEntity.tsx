import { LineChartOutlined } from '@ant-design/icons';
import * as React from 'react';
import { Chart, EntityType, SearchResult } from '../../../types.generated';
import { Direction } from '../../lineage/types';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { getLogoFromPlatform } from './getLogoFromPlatform';
import { ChartPreview } from './preview/ChartPreview';
import ChartProfile from './profile/ChartProfile';

export default function getChildren(entity: Chart, direction: Direction | null): Array<string> {
    if (direction === Direction.Upstream) {
        return entity.info?.inputs?.map((input) => input.urn) || [];
    }

    if (direction === Direction.Downstream) {
        return [];
    }

    return [];
}

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

    isLineageEnabled = () => true;

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

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as Chart);
    };

    getLineageVizConfig = (entity: Chart) => {
        return {
            urn: entity.urn,
            name: entity.info?.name || '',
            type: EntityType.Chart,
            upstreamChildren: getChildren(entity, Direction.Upstream),
            downstreamChildren: getChildren(entity, Direction.Downstream),
            icon: getLogoFromPlatform(entity.tool),
        };
    };
}
