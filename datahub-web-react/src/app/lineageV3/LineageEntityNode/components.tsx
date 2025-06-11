import { colors } from '@components';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

export const ExpandContractButton = styled.div<{ expandOnHover?: boolean }>`
    background-color: ${colors.white};
    color: ${colors.violet[500]};
    cursor: pointer;
    font-size: 18px;

    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 4px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);

    position: absolute;
    top: 50%;

    display: flex;
    align-items: center;

    max-width: 26px;
    transition: max-width 0.3s ease-in-out;
    overflow: hidden;

    :hover {
        ${(props) => props.expandOnHover && `max-width: 52px;`}
    }
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
    line-height: 0;
    padding: 4px;

    :hover {
        background-color: ${colors.gray[1600]};
    }
`;
