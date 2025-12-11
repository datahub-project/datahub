/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BuildOutlined } from '@ant-design/icons';
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
            return <>Has Siblings</>;
        }
        return <>Has No Siblings</>;
    };
}
