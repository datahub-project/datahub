import React from 'react';
import { CloseCircleOutlined } from '@ant-design/icons';
import { FilterRenderer } from '../FilterRenderer';
import { FilterRenderProps } from '../types';
import { HasFailingAssertionsFilter } from './HasFailingAssertionsFilter';

export class HasFailingAssertionsRenderer implements FilterRenderer {
    field = 'hasFailingAssertions';

    render = (props: FilterRenderProps) => <HasFailingAssertionsFilter {...props} icon={this.icon()} />;

    icon = () => <CloseCircleOutlined />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>Has Failing Assertions</>;
        }
        return <>Has Passing Assertions</>;
    };
}
