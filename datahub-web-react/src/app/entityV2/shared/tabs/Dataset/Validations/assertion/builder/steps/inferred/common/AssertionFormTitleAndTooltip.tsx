import React from 'react';

import { AssertionTooltip } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/AssertionTooltip';

export interface AssertionFormTitleAndTooltipProps {
    formTitle: string;
    tooltipTitle: string;
    tooltipDescription: string;
    titleClassName?: string;
}

export const AssertionFormTitleAndTooltip = ({
    formTitle,
    tooltipTitle,
    tooltipDescription,
    titleClassName,
}: AssertionFormTitleAndTooltipProps) => {
    return (
        <>
            <span style={{ marginRight: '8px' }} className={titleClassName}>
                {formTitle}
            </span>{' '}
            <AssertionTooltip title={tooltipTitle} description={tooltipDescription} />
        </>
    );
};
