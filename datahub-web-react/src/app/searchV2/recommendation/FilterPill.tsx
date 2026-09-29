import { Pill, Tooltip } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { RecommendedFilter } from '@app/searchV2/recommendation/types';

type Props = {
    filter: RecommendedFilter;
    onToggle: () => void;
};

export const FilterPill = ({ filter, onToggle }: Props) => {
    const { t } = useTranslation('search');
    // Convert the color to a valid ColorValues enum value, defaulting to gray if not found

    // Convert ReactNode label to string
    const labelString = typeof filter.label === 'string' ? filter.label : filter.label?.toString() || '';

    return (
        <Tooltip
            showArrow={false}
            placement="top"
            title={
                <Trans
                    t={t}
                    i18nKey="recommendation.viewResultsIn"
                    values={{ label: labelString }}
                    components={{ bold: <b /> }}
                />
            }
        >
            <Pill
                label={labelString}
                customIconRenderer={() => filter.icon}
                color="gray"
                variant="outline"
                clickable
                onPillClick={onToggle}
            />
        </Tooltip>
    );
};
