import * as React from 'react';
import { Dashboard, EntityType } from '../../../types.generated';
import { Entity, PreviewType } from '../Entity';
import { DashboardPreview } from './preview/DashboardPreview';
import DashboardProfile from './profile/DashboardProfile';

/**
 * Definition of the DataHub Dashboard entity.
 */
export class DashboardEntity implements Entity<Dashboard> {
    type: EntityType = EntityType.Dashboard;

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

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
            />
        );
    };
}
