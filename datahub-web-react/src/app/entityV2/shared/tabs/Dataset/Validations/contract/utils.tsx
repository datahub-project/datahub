import React from 'react';
import {
    CheckOutlined,
    ClockCircleOutlined,
    CloseOutlined,
    ExclamationCircleFilled,
    StopOutlined,
} from '@ant-design/icons';
import { Assertion, AssertionType, DataContract, DataContractState } from '../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../constants';
import { FAILURE_COLOR_HEX, SUCCESS_COLOR_HEX, WARNING_COLOR_HEX } from '../acrylUtils';
import { DataContractCategoryType } from './builder/types';
import { AssertionStatusSummary } from '../acrylTypes';

export const getContractSummaryIcon = (state: DataContractState, summary: AssertionStatusSummary) => {
    if (state === DataContractState.Pending) {
        return <ClockCircleOutlined style={{ color: ANTD_GRAY[6], fontSize: 28 }} />;
    }
    if (summary.total === 0) {
        return <StopOutlined style={{ color: ANTD_GRAY[6], fontSize: 28 }} />;
    }
    if (summary.passing === summary.total) {
        return <CheckOutlined style={{ color: SUCCESS_COLOR_HEX, fontSize: 28 }} />;
    }
    if (summary.failing > 0) {
        return <CloseOutlined style={{ color: FAILURE_COLOR_HEX, fontSize: 28 }} />;
    }
    if (summary.erroring > 0) {
        return <ExclamationCircleFilled style={{ color: WARNING_COLOR_HEX, fontSize: 28 }} />;
    }
    return <StopOutlined style={{ color: ANTD_GRAY[6], fontSize: 28 }} />;
};

export const getContractSummaryTitle = (state: DataContractState, summary: AssertionStatusSummary) => {
    if (state === DataContractState.Pending) {
        return 'This contract is pending implementation';
    }
    if (summary.total === 0) {
        return 'This contract has not yet been validated';
    }
    if (summary.passing === summary.total) {
        return 'This dataset is meeting its contract';
    }
    if (summary.failing > 0) {
        return 'This dataset is not meeting its contract';
    }
    if (summary.erroring > 0) {
        return 'Unable to determine contract status';
    }
    return 'This contract has not yet been validated';
};

export const getContractSummaryMessage = (state: DataContractState, summary: AssertionStatusSummary) => {
    if (state === DataContractState.Pending) {
        return 'This may take some time. Come back later!';
    }
    if (summary.total === 0) {
        return 'No contract assertions have been run yet';
    }
    if (summary.passing === summary.total) {
        return 'All contract assertions are passing';
    }
    if (summary.failing > 0) {
        return 'Some contract assertions are failing';
    }
    if (summary.erroring > 0) {
        return 'Some contract assertions are completing with errors';
    }
    return 'No contract assertions have been run yet';
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

export const DATA_QUALITY_ASSERTION_TYPES = new Set([
    AssertionType.Volume,
    AssertionType.Sql,
    AssertionType.Field,
    AssertionType.Dataset,
    AssertionType.Custom,
]);
