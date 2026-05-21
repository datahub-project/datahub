import { GitFork } from '@phosphor-icons/react/dist/csr/GitFork';
import React from 'react';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasSiblingsFilter } from '@app/searchV2/filters/render/siblings/HasSiblingsFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';

import { AppConfig } from '@types';

export class HasSiblingsRenderer implements FilterRenderer {
    field = 'hasSiblings';

    name = 'Siblings';

    canBeRendered = (config?: AppConfig) => {
        return !!config?.featureFlags?.showHasSiblingsFilter;
    };

    render = (props: FilterRenderProps) => {
        if (!this.canBeRendered(props.config)) {
            return <></>;
        }
        return <HasSiblingsFilter {...props} />;
    };

    icon = () => <GitFork size={14} />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>Has Siblings</>;
        }
        return <>Has No Siblings</>;
    };
}
