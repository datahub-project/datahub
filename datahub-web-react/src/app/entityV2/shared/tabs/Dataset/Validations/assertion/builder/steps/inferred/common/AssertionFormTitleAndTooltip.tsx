import { colors } from '@components';
import React from 'react';

import { AssertionTooltip } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/AssertionTooltip';

export interface AssertionFormTitleAndTooltipProps {
    formTitle: string;
    formSubtitle?: string;
    tooltipTitle: string;
    tooltipDescription: string;
    titleClassName?: string;
}

export const AssertionFormTitleAndTooltip = ({
    formTitle,
    formSubtitle,
    tooltipTitle,
    tooltipDescription,
    titleClassName,
}: AssertionFormTitleAndTooltipProps) => {
    return (
        <div style={{ display: 'flex', flexDirection: 'column' }}>
            <div>
                <span style={{ marginRight: '8px' }} className={titleClassName}>
                    {formTitle}
                </span>{' '}
                <AssertionTooltip title={tooltipTitle} description={tooltipDescription} />
            </div>
            {formSubtitle && <span style={{ color: colors.gray[400] }}>{formSubtitle}</span>}
        </div>
    );
};
