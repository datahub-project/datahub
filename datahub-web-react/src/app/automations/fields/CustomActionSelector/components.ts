import styled from 'styled-components';

import { sharedStyles } from '@app/automations/sharedComponents';

/*
 * Containers
 */

export const EmptyStateContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    margin-top: 16px;

    font-family: ${sharedStyles.fontFamily};

    & h3 {
        font-weight: 700;
    }
`;

export const CustomActionsContainer = styled.div`
    display: grid;
    grid-template-columns: 1fr;
    grid-template-rows: repeat(1fr);
`;

export const CustomActionBuilderContainer = styled.div`
    display: grid;
    grid-template-rows: repeat(1fr);
    grid-row-gap: 8px;

    border-radius: ${sharedStyles.borderRadius};
    border: 1px solid ${sharedStyles.borderColor};
    background: #f9fafc;
    padding: 8px;
`;

export const BuilderGroupContainer = styled.div`
    display: grid;
    grid-template-columns: 1fr;
    grid-template-rows: repeat(1fr);
    gap: 8px;

    &:not(:last-child) {
        margin-bottom: 16px;
    }
`;

export const BuilderGroupHeader = styled.div`
    display: flex;
    align-items: center;

    & h3 {
        color: ${sharedStyles.headingColor};
        font-size: 14px;
        font-weight: 700;
        margin: 0;
    }

    & svg {
        margin-right: 8px;
    }
`;

export const ActionGroupContainer = styled.div`
    display: grid;
    grid-template-columns: 1fr;
    grid-template-rows: repeat(1fr);
    grid-row-gap: 8px;
    margin-bottom: 4px;
`;

export const ActionSelectorContainer = styled.div`
    // Root Layout
    display: grid;
    grid-template-columns: 0.5fr 1.5fr 0.1fr;
    grid-template-rows: 1fr;
    grid-column-gap: 8px;
    grid-row-gap: 0px;
    align-items: center;

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

export const ActionsButtonContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;
