import styled from 'styled-components';

import { sharedStyles } from '../sharedComponents';

export const Step = styled.div`
    margin-bottom: 20px;
    font-family: ${sharedStyles.fontFamily};
`;

export const StepHeader = styled.div`
    & h2 {
        color: ${sharedStyles.subHeadingColor};
        font-size: 18px;
        font-weight: 700;
        line-height: normal;
        margin: 0;
    }

    & p {
        color: ${sharedStyles.contentColor};
        font-size: 14px;
        font-style: normal;
        font-weight: 500;
        line-height: 20px;
    }
`;

export const StepField = styled.div`
    margin-bottom: 8px;

    & label:not(.ant-checkbox-wrapper) {
        display: block;
        font-weight: 700;
        margin-bottom: 0.25rem;
    }

    & .ant-select,
    & input {
        width: 100%;
    }

    & textarea {
        resize: none;
    }
`;

export const StepButtons = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

export const InputWithButton = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;
