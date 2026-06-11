import { CheckOutlined, CloseOutlined, InfoCircleOutlined } from '@ant-design/icons';
import i18next from 'i18next';
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
    get [AssertionType.Freshness]() {
        return i18next.t('entity.profile.validations:assertionTypeSubtitle.freshness');
    },
    get [AssertionType.Volume]() {
        return i18next.t('entity.profile.validations:assertionTypeSubtitle.volume');
    },
    get [AssertionType.Field]() {
        return i18next.t('entity.profile.validations:assertionTypeSubtitle.field');
    },
    get [AssertionType.DataSchema]() {
        return i18next.t('entity.profile.validations:assertionTypeSubtitle.dataSchema');
    },
    get [AssertionType.Custom]() {
        return i18next.t('entity.profile.validations:assertionTypeSubtitle.custom');
    },
    get [AssertionType.Sql]() {
        return i18next.t('entity.profile.validations:assertionTypeSubtitle.sql');
    },
    get [AssertionType.Dataset]() {
        return i18next.t('entity.profile.validations:assertionTypeSubtitle.dataset');
    },
};

export const getAssertionSummaryCardHeaderByStatus = (colors: ColorTheme) => ({
    passing: {
        color: colors.textSuccess,
        backgroundColor: colors.bgSurfaceSuccess,
        resultType: AssertionResultType.Success,
        icon: <CheckOutlined />,
        text: i18next.t('entity.profile.validations:status.passing'),
        headerComponent: (
            <StyledCardTitle background={colors.bgSurfaceSuccess} color={colors.textSuccess}>
                <CheckOutlined /> {i18next.t('entity.profile.validations:status.passing')}
            </StyledCardTitle>
        ),
    },
    failing: {
        color: colors.textError,
        backgroundColor: colors.bgSurfaceError,
        resultType: AssertionResultType.Failure,
        icon: <CloseOutlined />,
        text: i18next.t('entity.profile.validations:status.failing'),
        headerComponent: (
            <StyledCardTitle background={colors.bgSurfaceError} color={colors.textError}>
                <CloseOutlined /> {i18next.t('entity.profile.validations:status.failing')}
            </StyledCardTitle>
        ),
    },
    erroring: {
        color: colors.textWarning,
        backgroundColor: colors.bgSurfaceWarning,
        resultType: AssertionResultType.Error,
        icon: <InfoCircleOutlined />,
        text: i18next.t('entity.profile.validations:status.errors'),
        headerComponent: (
            <StyledCardTitle background={colors.bgSurfaceWarning} color={colors.textWarning}>
                <InfoCircleOutlined /> {i18next.t('entity.profile.validations:status.error')}
            </StyledCardTitle>
        ),
    },
    [NO_RUNNING_STATE]: {
        color: colors.textTertiary,
        backgroundColor: colors.bgSurface,
        resultType: null,
        icon: <InfoCircleOutlined />,
        text: i18next.t('entity.profile.validations:status.zeroRunning'),
        headerComponent: (
            <StyledCardTitle background={colors.bgSurface} color={colors.textTertiary}>
                <InfoCircleOutlined /> {i18next.t('entity.profile.validations:status.noRuns')}
            </StyledCardTitle>
        ),
    },
});
