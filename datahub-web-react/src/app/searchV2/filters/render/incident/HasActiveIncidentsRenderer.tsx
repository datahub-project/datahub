import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import i18next from 'i18next';
import React from 'react';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasActiveIncidentsFilter } from '@app/searchV2/filters/render/incident/HasActiveIncidentsFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';

export class HasActiveIncidentsRenderer implements FilterRenderer {
    field = 'hasActiveIncidents';

    get name() {
        return i18next.t('search:filters.incidents.title');
    }

    render = (props: FilterRenderProps) => <HasActiveIncidentsFilter {...props} />;

    icon = () => <Warning size={14} />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>{i18next.t('search:filters.incidents.hasActiveLabel')}</>;
        }
        return <>{i18next.t('search:filters.incidents.hasResolvedLabel')}</>;
    };
}
