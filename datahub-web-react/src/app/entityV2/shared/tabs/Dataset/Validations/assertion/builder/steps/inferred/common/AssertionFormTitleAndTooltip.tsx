import React from 'react';
import { AssertionTooltip } from './AssertionTooltip';

export interface AssertionFormTitleAndTooltipProps {
    formTitle: string;
    tooltipTitle: string;
    tooltipDescription: string;
}

export const AssertionFormTitleAndTooltip = ({
    formTitle,
    tooltipTitle,
    tooltipDescription,
}: AssertionFormTitleAndTooltipProps) => {
    return (
        <>
            <span style={{ marginRight: '8px' }}>{formTitle}</span>{' '}
            <AssertionTooltip title={tooltipTitle} description={tooltipDescription} />
        </>
    );
};
