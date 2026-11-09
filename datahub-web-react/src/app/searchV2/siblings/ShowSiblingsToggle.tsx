import { Switch } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { useShowSiblings } from '@app/searchV2/siblings/useShowSiblings';

/** Search filter-bar switch that lists each result's siblings under it. Off by default. */
export default function ShowSiblingsToggle() {
    const { t } = useTranslation('search');
    const [enabled, setEnabled] = useShowSiblings();
    return (
        <Switch
            label={t('siblings.showSiblings')}
            labelPosition="right"
            checked={enabled}
            isChecked={enabled}
            onChange={(e) => setEnabled(e.target.checked)}
            data-testid="show-siblings-toggle"
        />
    );
}
