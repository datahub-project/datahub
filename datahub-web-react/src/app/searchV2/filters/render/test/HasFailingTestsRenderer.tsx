import { CloseCircleOutlined } from '@ant-design/icons';
import React from 'react';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasFailingTestsFilter } from '@app/searchV2/filters/render/test/HasFailingTestsFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';

export class HasFailingTestsRenderer implements FilterRenderer {
    field = 'hasFailingTests';

    render = (props: FilterRenderProps) => <HasFailingTestsFilter {...props} icon={this.icon()} />;

    icon = () => <CloseCircleOutlined />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>Has Failing Tests</>;
        }
        return <>Has Passing Tests</>;
    };
}
