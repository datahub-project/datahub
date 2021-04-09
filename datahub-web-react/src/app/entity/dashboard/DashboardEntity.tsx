import { DashboardFilled, DashboardOutlined } from '@ant-design/icons';
import * as React from 'react';
import { Dashboard, EntityType, SearchResult } from '../../../types.generated';
import { Direction } from '../../lineage/types';
import { getLogoFromPlatform } from '../chart/getLogoFromPlatform';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { DashboardPreview } from './preview/DashboardPreview';
import DashboardProfile from './profile/DashboardProfile';

export default function getChildren(entity: Dashboard, direction: Direction | null): Array<string> {
    if (direction === Direction.Upstream) {
        return entity.info?.charts.map((chart) => chart.urn) || [];
    }

    if (direction === Direction.Downstream) {
        return [];
    }

    return [];
}

/**
 * Definition of the DataHub Dashboard entity.
 */
export class DashboardEntity implements Entity<Dashboard> {
    type: EntityType = EntityType.Dashboard;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DashboardOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DashboardFilled style={{ fontSize, color: 'rgb(144 163 236)' }} />;
        }

        return (
            <DashboardOutlined
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

    getPathName = () => 'dashboard';

    getCollectionName = () => 'Dashboards';

    renderProfile = (urn: string) => <DashboardProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: Dashboard) => {
        return (
            <DashboardPreview
                urn={data.urn}
                platform={data.tool}
                name={data.info?.name}
                description={data.info?.description}
                access={data.info?.access}
                tags={data.globalTags || undefined}
                owners={data.ownership?.owners}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as Dashboard);
    };

    getLineageVizConfig = (entity: Dashboard) => {
        return {
            urn: entity.urn,
            name: entity.info?.name || '',
            type: EntityType.Dashboard,
            upstreamChildren: getChildren(entity, Direction.Upstream),
            downstreamChildren: getChildren(entity, Direction.Downstream),
            icon: getLogoFromPlatform(entity.tool),
        };
    };
}
