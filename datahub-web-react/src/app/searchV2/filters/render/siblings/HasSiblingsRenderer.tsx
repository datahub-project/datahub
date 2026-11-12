import { GitFork } from '@phosphor-icons/react/dist/csr/GitFork';
import i18next from 'i18next';
import React from 'react';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasSiblingsFilter } from '@app/searchV2/filters/render/siblings/HasSiblingsFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';
import { AppConfigWithoutPolicyPrivileges } from '@src/appConfigContext';

export class HasSiblingsRenderer implements FilterRenderer {
    field = 'hasSiblings';

    get name() {
        return i18next.t('search:filters.siblings.title');
    }

    canBeRendered = (config?: AppConfigWithoutPolicyPrivileges) => {
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
            return <>{i18next.t('search:filters.siblings.hasSiblingsLabel')}</>;
        }
        return <>{i18next.t('search:filters.siblings.hasNoSiblingsLabel')}</>;
    };
}
