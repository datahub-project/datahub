import styled from 'styled-components';

import { H3, P, sharedStyles } from '@app/automations/sharedComponents';

const predicateSelectHeight = '16px';

/*
 * Containers
 */

export const EmptyStateContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    margin: 16px 0;

    border-radius: ${sharedStyles.borderRadius};
    border: 1px solid ${sharedStyles.borderColor};
    background: #f9fafc;
    padding: 24px;

    font-family: ${sharedStyles.fontFamily};

    & h3 {
        font-weight: 700;
    }

    & p {
        color: ${sharedStyles.contentColor};
        font-size: 14px;
        font-weight: 400;
    }
`;

export const ConditionsContainer = styled.div`
    display: grid;
    grid-template-rows: repeat(1fr);
    gap: 8px;
`;

export const RuleSelectorContainer = styled.div<{ noInput?: boolean }>`
    border-radius: ${sharedStyles.borderRadius};
    border: 1px solid ${sharedStyles.darkBorderColor};
    background: #f9fafc;
    padding: 8px;

    // Root Layout
    display: grid;
    grid-template-columns: 0.05fr 1fr 0.5fr 1fr 0.1fr;
    grid-template-rows: 1fr;
    grid-column-gap: 8px;
    grid-row-gap: 0px;
    align-items: center;

    // No input layout
    ${(props) =>
        props.noInput &&
        `
		grid-template-columns: 0.05fr 4fr 1fr 0.1fr;
	`}

    // Inner div containers
	& > div {
        display: flex;
        align-items: center;
    }

    // Last div container
    & > div:last-child {
        justify-content: flex-end;
    }
`;

export const OperatorSelectorContainer = styled.div`
    & .ant-select,
    & .ant-select-selector,
    & .ant-select-selection-search {
        height: ${predicateSelectHeight};
    }

    & .ant-select,
    & .ant-select-selector,
    & .ant-select-selection-item,
    & .ant-select-single .ant-select-selector .ant-select-selection-item,
    .ant-select-single .ant-select-selector .ant-select-selection-placeholder {
        line-height: ${predicateSelectHeight};
    }

    & .ant-select-single:not(.ant-select-customize-input) .ant-select-selector::after {
        line-height: 0;
    }

    & .ant-select {
        height: auto;
        width: 60px;
        margin: 8px 0 16px;

        & .ant-select-selector {
            height: auto;
            border-radius: 20px;
            background: #e5e2f8;
            border: none;
            padding: 4px 8px;

            color: #374066;
            font-family: ${sharedStyles.fontFamily};
            font-size: 12px;
            font-style: normal;
            font-weight: 500;
            line-height: ${predicateSelectHeight};
        }

        & .ant-select-arrow {
            display: block;
            top: 13px;
        }
    }
`;

export const ConditionGroupContainer = styled.div`
    display: grid;
    grid-template-rows: repeat(1fr);
    grid-row-gap: 8px;
    padding: 16px;

    border-radius: 8px;
    border: 1px solid ${sharedStyles.borderColor};
    background: #f6f6f6;
`;

export const ConditionsActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

export const RuleSelectorActionsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    gap: 8px;
`;

export const GroupAddContainer = styled.div`
    margin-top: 8px;
`;

/*
 * Typography
 */

export const MatchingAssetHeader = styled(H3);

export const MatchingAssetText = styled(P)`
    margin-bottom: 0;
    margin-top: 4px;
`;
