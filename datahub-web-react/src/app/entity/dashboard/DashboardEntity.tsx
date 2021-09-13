import { DashboardFilled, DashboardOutlined } from '@ant-design/icons';
import * as React from 'react';
import { Dashboard, EntityType, SearchResult } from '../../../types.generated';
import { Direction } from '../../lineage/types';
import { getLogoFromPlatform } from '../../shared/getLogoFromPlatform';
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

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M924.8 385.6a446.7 446.7 0 00-96-142.4 446.7 446.7 0 00-142.4-96C631.1 123.8 572.5 112 512 112s-119.1 11.8-174.4 35.2a446.7 446.7 0 00-142.4 96 446.7 446.7 0 00-96 142.4C75.8 440.9 64 499.5 64 560c0 132.7 58.3 257.7 159.9 343.1l1.7 1.4c5.8 4.8 13.1 7.5 20.6 7.5h531.7c7.5 0 14.8-2.7 20.6-7.5l1.7-1.4C901.7 817.7 960 692.7 960 560c0-60.5-11.9-119.1-35.2-174.4zM761.4 836H262.6A371.12 371.12 0 01140 560c0-99.4 38.7-192.8 109-263 70.3-70.3 163.7-109 263-109 99.4 0 192.8 38.7 263 109 70.3 70.3 109 163.7 109 263 0 105.6-44.5 205.5-122.6 276zM623.5 421.5a8.03 8.03 0 00-11.3 0L527.7 506c-18.7-5-39.4-.2-54.1 14.5a55.95 55.95 0 000 79.2 55.95 55.95 0 0079.2 0 55.87 55.87 0 0014.5-54.1l84.5-84.5c3.1-3.1 3.1-8.2 0-11.3l-28.3-28.3zM490 320h44c4.4 0 8-3.6 8-8v-80c0-4.4-3.6-8-8-8h-44c-4.4 0-8 3.6-8 8v80c0 4.4 3.6 8 8 8zm260 218v44c0 4.4 3.6 8 8 8h80c4.4 0 8-3.6 8-8v-44c0-4.4-3.6-8-8-8h-80c-4.4 0-8 3.6-8 8zm12.7-197.2l-31.1-31.1a8.03 8.03 0 00-11.3 0l-56.6 56.6a8.03 8.03 0 000 11.3l31.1 31.1c3.1 3.1 8.2 3.1 11.3 0l56.6-56.6c3.1-3.1 3.1-8.2 0-11.3zm-458.6-31.1a8.03 8.03 0 00-11.3 0l-31.1 31.1a8.03 8.03 0 000 11.3l56.6 56.6c3.1 3.1 8.2 3.1 11.3 0l31.1-31.1c3.1-3.1 3.1-8.2 0-11.3l-56.6-56.6zM262 530h-80c-4.4 0-8 3.6-8 8v44c0 4.4 3.6 8 8 8h80c4.4 0 8-3.6 8-8v-44c0-4.4-3.6-8-8-8z" />
            );
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
                description={data.editableProperties?.description || data.info?.description}
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
            platform: entity.tool,
        };
    };

    displayName = (data: Dashboard) => {
        return data.info?.name || data.urn;
    };
}
