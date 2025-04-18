import React from 'react';
import { BuildOutlined } from '@ant-design/icons';
import { FilterRenderer } from '../FilterRenderer';
import { FilterRenderProps } from '../types';
import { HasSiblingsFilter } from './HasSiblingsFilter';

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
