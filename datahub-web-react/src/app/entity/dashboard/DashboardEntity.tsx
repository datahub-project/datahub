import { DashboardFilled, DashboardOutlined } from '@ant-design/icons';
import * as React from 'react';
import { Dashboard, EntityType } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { DashboardPreview } from './preview/DashboardPreview';
import DashboardProfile from './profile/DashboardProfile';

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
}
