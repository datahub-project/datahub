import { WarningOutlined } from '@ant-design/icons';
import i18next from 'i18next';
import React from 'react';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasActiveIncidentsFilter } from '@app/searchV2/filters/render/incident/HasActiveIncidentsFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';

export class HasActiveIncidentsRenderer implements FilterRenderer {
    field = 'hasActiveIncidents';

    render = (props: FilterRenderProps) => <HasActiveIncidentsFilter {...props} icon={this.icon()} />;

    icon = () => <WarningOutlined />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>{i18next.t('search:filters.incidents.hasActiveLabel')}</>;
        }
        return <>{i18next.t('search:filters.incidents.hasResolvedLabel')}</>;
    };
}
