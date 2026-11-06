import { Pill } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ColorOptions } from '@components/theme/config';

import { getAssertionResultSeverityDisplay } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/assertionResultSeverityUtils';
import {
    ResultStatusType,
    getResultStatusText,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';

import { AssertionResult, AssertionResultType } from '@types';

const RESULT_PILL_COLORS: Record<AssertionResultType, ColorOptions> = {
    [AssertionResultType.Success]: 'green',
    [AssertionResultType.Failure]: 'red',
    [AssertionResultType.Error]: 'yellow',
    [AssertionResultType.Init]: 'blue',
};

const SeverityIcon = styled.svg`
    width: 16px;
    height: 16px;

    path {
        fill: currentColor;
    }
`;

type Props = {
    result?: AssertionResult;
    type?: ResultStatusType;
};

export const AssertionResultPill = ({ result, type = ResultStatusType.LATEST }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const resultType = result?.type;
    const text = (resultType && getResultStatusText(resultType, type)) || t('resultPill.noResultsYet');
    const severityDisplay = getAssertionResultSeverityDisplay(result);
    return (
        <Pill
            label={text}
            variant="filled"
            color={resultType ? RESULT_PILL_COLORS[resultType] : 'gray'}
            customRightIconRenderer={
                severityDisplay
                    ? () => <SeverityIcon as={severityDisplay.icon} aria-label={severityDisplay.label} />
                    : undefined
            }
        />
    );
};
