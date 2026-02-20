import { CheckOutlined, CloseOutlined, InfoCircleOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { NO_RUNNING_STATE } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import ColorTheme from '@conf/theme/colorThemes/types';
import { AssertionResultType, AssertionType } from '@src/types.generated';

const StyledCardTitle = styled.div<{ background: string; color: string }>`
    background: ${({ background }) => background};
    color: ${({ color }) => color};
    padding: 8px;
    font-weight: 700;
    padding-left: 24px;
    gap: 8px;
    display: flex;
    align-items: center;
    font-size: 12px;
`;

export const ASSERTION_TYPE_TO_HEADER_SUBTITLE: Record<AssertionType, string> = {
    [AssertionType.Freshness]: 'Verifies when this dataset should be updated.',
    [AssertionType.Volume]: 'Verifies the row count of this dataset.',
    [AssertionType.Field]: 'Verifies the validity of a column.',
    [AssertionType.DataSchema]: 'Verifies the schema of this dataset.',
    [AssertionType.Custom]: 'A custom externally reported assertion.',
    [AssertionType.Sql]: 'Verifies using custom SQL rules.',
    [AssertionType.Dataset]: 'An external assertion.',
};

export const getAssertionSummaryCardHeaderByStatus = (colors: ColorTheme) => ({
    passing: {
        color: colors.textSuccess,
        backgroundColor: colors.bgSurfaceSuccess,
        resultType: AssertionResultType.Success,
        icon: <CheckOutlined />,
        text: 'Passing',
        headerComponent: (
            <StyledCardTitle background={colors.bgSurfaceSuccess} color={colors.textSuccess}>
                <CheckOutlined /> Passing
            </StyledCardTitle>
        ),
    },
    failing: {
        color: colors.textError,
        backgroundColor: colors.bgSurfaceError,
        resultType: AssertionResultType.Failure,
        icon: <CloseOutlined />,
        text: 'Failing',
        headerComponent: (
            <StyledCardTitle background={colors.bgSurfaceError} color={colors.textError}>
                <CloseOutlined /> Failing
            </StyledCardTitle>
        ),
    },
    erroring: {
        color: colors.textWarning,
        backgroundColor: colors.bgSurfaceWarning,
        resultType: AssertionResultType.Error,
        icon: <InfoCircleOutlined />,
        text: 'Errors',
        headerComponent: (
            <StyledCardTitle background={colors.bgSurfaceWarning} color={colors.textWarning}>
                <InfoCircleOutlined /> Error
            </StyledCardTitle>
        ),
    },
    [NO_RUNNING_STATE]: {
        color: colors.textTertiary,
        backgroundColor: colors.bgSurface,
        resultType: null,
        icon: <InfoCircleOutlined />,
        text: '0 Running',
        headerComponent: (
            <StyledCardTitle background={colors.bgSurface} color={colors.textTertiary}>
                <InfoCircleOutlined /> No runs
            </StyledCardTitle>
        ),
    },
});
