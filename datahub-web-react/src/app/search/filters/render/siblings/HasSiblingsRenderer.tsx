import { BuildOutlined } from '@ant-design/icons';
import i18next from 'i18next';
import React from 'react';

import { FilterRenderer } from '@app/search/filters/render/FilterRenderer';
import { HasSiblingsFilter } from '@app/search/filters/render/siblings/HasSiblingsFilter';
import { FilterRenderProps } from '@app/search/filters/render/types';

export class HasSiblingsRenderer implements FilterRenderer {
    field = 'hasSiblings';

    render = (props: FilterRenderProps) => {
        if (!props.config?.featureFlags?.showHasSiblingsFilter) {
            return <></>;
        }
        return <HasSiblingsFilter {...props} icon={this.icon()} />;
    };

    icon = () => <BuildOutlined />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>{i18next.t('search:filters.siblings.hasSiblingsLabel')}</>;
        }
        return <>{i18next.t('search:filters.siblings.hasNoSiblingsLabel')}</>;
    };
}
