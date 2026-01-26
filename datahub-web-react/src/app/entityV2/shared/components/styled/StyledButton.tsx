import { Button } from 'antd';
import styled, { css } from 'styled-components';

export default styled(Button)<{ $hasBorder?: boolean; $hasHover?: boolean }>`
    padding-top: 5px;
    padding-bottom: 5px;
    padding-right: 16px;
    padding-left: 16px;
    margin: 0px 10px 0px 10px;
    box-shadow: 0px 0px 4px 0px #0000001a;
    border: ${(props) =>
        props.$hasBorder === false ? 'none' : `1px solid ${props.theme.styles['primary-color-lightest']};`
    }

    font-size: 12px;
    font-weight: 500;
    line-height: 20px;
    vertical-align: top;
    border-radius: 5px;
    color: ${(props) => props.theme.styles['primary-color']};

    ${(props) =>
        props.$hasHover &&
        css`
            &:hover {
                color: ${(props) => props.theme.styles['primary-color-darkest']};
                background-color: ${(props) => props.theme.styles['primary-color-lightest']};
            }
        `
    }
`;
