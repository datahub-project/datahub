import {
    CheckOutlined,
    ClockCircleOutlined,
    CloseOutlined,
    ExclamationCircleFilled,
    StopOutlined,
} from '@ant-design/icons';
import i18next from 'i18next';
import React from 'react';

import { AssertionStatusSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylTypes';
import { DataContractCategoryType } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/types';
import ColorTheme from '@src/conf/theme/colorThemes/types';

import { Assertion, AssertionType, DataContract, DataContractState } from '@types';

export const getContractSummaryIcon = (
    state: DataContractState,
    summary: AssertionStatusSummary,
    colors: ColorTheme,
) => {
    if (state === DataContractState.Pending) {
        return <ClockCircleOutlined style={{ color: colors.iconDisabled, fontSize: 28 }} />;
    }
    if (summary.total === 0) {
        return <StopOutlined style={{ color: colors.iconDisabled, fontSize: 28 }} />;
    }
    if (summary.passing === summary.total) {
        return <CheckOutlined style={{ color: colors.iconSuccess, fontSize: 28 }} />;
    }
    if (summary.failing > 0) {
        return <CloseOutlined style={{ color: colors.iconError, fontSize: 28 }} />;
    }
    if (summary.erroring > 0) {
        return <ExclamationCircleFilled style={{ color: colors.iconWarning, fontSize: 28 }} />;
    }
    return <StopOutlined style={{ color: colors.iconDisabled, fontSize: 28 }} />;
};

export const getContractSummaryTitle = (state: DataContractState, summary: AssertionStatusSummary) => {
    if (state === DataContractState.Pending) {
        return i18next.t('entity.profile.validations:contractStatus.pending');
    }
    if (summary.total === 0) {
        return i18next.t('entity.profile.validations:contractStatus.noAssertionsRun');
    }
    if (summary.passing === summary.total) {
        return i18next.t('entity.profile.validations:contractStatus.passing');
    }
    if (summary.failing > 0) {
        return i18next.t('entity.profile.validations:contractStatus.failing');
    }
    if (summary.erroring > 0) {
        return i18next.t('entity.profile.validations:contractStatus.erroring');
    }
    return i18next.t('entity.profile.validations:contractStatus.noAssertionsDefault');
};

export const getContractSummaryMessage = (state: DataContractState, summary: AssertionStatusSummary) => {
    if (state === DataContractState.Pending) {
        return i18next.t('entity.profile.validations:contractMessage.pending');
    }
    if (summary.total === 0) {
        return i18next.t('entity.profile.validations:contractMessage.noAssertionsRun');
    }
    if (summary.passing === summary.total) {
        return i18next.t('entity.profile.validations:contractMessage.allPassing');
    }
    if (summary.failing > 0) {
        return i18next.t('entity.profile.validations:contractMessage.failing');
    }
    if (summary.erroring > 0) {
        return i18next.t('entity.profile.validations:contractMessage.erroring');
    }
    return i18next.t('entity.profile.validations:contractMessage.noAssertionsDefault');
};

/**
 * Returns true if a given assertion is part of a given contract, false otherwise.
 */
export const isAssertionPartOfContract = (assertion: Assertion, contract: DataContract) => {
    if (contract.properties?.dataQuality?.some((c) => c.assertion.urn === assertion?.urn)) {
        return true;
    }
    if (contract.properties?.schema?.some((c) => c.assertion.urn === assertion?.urn)) {
        return true;
    }
    if (contract.properties?.freshness?.some((c) => c.assertion.urn === assertion?.urn)) {
        return true;
    }
    return false;
};

/**
 * Retrieves the high level contract category - schema, freshness, or data quality - given an assertion
 */
export const getDataContractCategoryFromAssertion = (assertion: Assertion) => {
    if (
        assertion.info?.type === AssertionType.Dataset ||
        assertion.info?.type === AssertionType.Volume ||
        assertion.info?.type === AssertionType.Field ||
        assertion.info?.type === AssertionType.Sql
    ) {
        return DataContractCategoryType.DATA_QUALITY;
    }
    if (assertion.info?.type === AssertionType.Freshness) {
        return DataContractCategoryType.FRESHNESS;
    }
    if (assertion.info?.type === AssertionType.DataSchema) {
        return DataContractCategoryType.SCHEMA;
    }
    return DataContractCategoryType.DATA_QUALITY;
};

export const getDataContractCategoryLabel = (category: DataContractCategoryType): string => {
    const labels: Record<DataContractCategoryType, () => string> = {
        [DataContractCategoryType.FRESHNESS]: () => i18next.t('entity.profile.validations:contractCategory.freshness'),
        [DataContractCategoryType.SCHEMA]: () => i18next.t('entity.profile.validations:contractCategory.schema'),
        [DataContractCategoryType.DATA_QUALITY]: () =>
            i18next.t('entity.profile.validations:contractCategory.dataQuality'),
    };
    return labels[category]?.() ?? category;
};

export const DATA_QUALITY_ASSERTION_TYPES = new Set([
    AssertionType.Volume,
    AssertionType.Sql,
    AssertionType.Field,
    AssertionType.Dataset,
    AssertionType.Custom,
]);
