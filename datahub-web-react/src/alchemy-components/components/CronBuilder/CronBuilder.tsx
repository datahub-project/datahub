import React from 'react';
import Cron from 'react-js-cron';
import 'react-js-cron/dist/styles.css';

import { CronBuilderContainer, CronDropdownGlobalStyles } from '@components/components/CronBuilder/components';
import { CronBuilderProps } from '@components/components/CronBuilder/types';

/**
 * `react-js-cron` editor with DataHub theming applied. Use it instead of importing
 * `react-js-cron` directly, so every cron editor gets the same styles.
 *
 * Not exported from the `@components` root, to keep `react-js-cron` out of the eager bundle.
 */
export function CronBuilder(props: CronBuilderProps) {
    return (
        <CronBuilderContainer>
            <CronDropdownGlobalStyles />
            <Cron {...props} />
        </CronBuilderContainer>
    );
}
