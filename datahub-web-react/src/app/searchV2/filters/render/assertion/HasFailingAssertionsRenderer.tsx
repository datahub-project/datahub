import { CloseCircleOutlined } from '@ant-design/icons';
import i18next from 'i18next';
import React from 'react';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasFailingAssertionsFilter } from '@app/searchV2/filters/render/assertion/HasFailingAssertionsFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';

export class HasFailingAssertionsRenderer implements FilterRenderer {
    field = 'hasFailingAssertions';

    render = (props: FilterRenderProps) => <HasFailingAssertionsFilter {...props} icon={this.icon()} />;

    icon = () => <CloseCircleOutlined />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>{i18next.t('search:filters.assertions.hasFailingLabel')}</>;
        }
        return <>{i18next.t('search:filters.assertions.hasPassingLabel')}</>;
    };
}
