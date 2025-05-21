import { BuildOutlined } from '@ant-design/icons';
import React from 'react';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasSiblingsFilter } from '@app/searchV2/filters/render/siblings/HasSiblingsFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';

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
            return <>Has Siblings</>;
        }
        return <>Has No Siblings</>;
    };
}
