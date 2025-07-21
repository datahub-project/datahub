import { colors } from '@components';
import styled from 'styled-components';

export const ExpandContractButton = styled.div<{ expandOnHover?: boolean }>`
    background-color: ${colors.white};
    color: ${colors.violet[500]};
    cursor: pointer;
    font-size: 18px;

    border-radius: 4px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);

    position: absolute;

    display: flex;
    align-items: center;

    overflow: hidden;
    transition: max-width 0.3s ease-in-out;

    ${(props) =>
        props.expandOnHover &&
        `
        max-width: 24px;    
        :hover {    
            max-width: 48px;
        }
    `}
`;

export const UpstreamWrapper = styled(ExpandContractButton)`
    right: calc(100% + 10px);
    transform: translateY(-50%) scaleX(-1);
`;

export const DownstreamWrapper = styled(ExpandContractButton)`
    left: calc(100% + 10px);
    transform: translateY(-50%);
`;

export const Button = styled.span`
    display: flex;
    align-items: center;
    font-size: 12px;

    line-height: 0;
    padding: 4px;

    :hover {
        background-color: ${colors.gray[1600]};
    }
`;
