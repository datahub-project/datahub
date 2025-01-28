import styled from 'styled-components';
import { ANTD_GRAY, LINEAGE_COLORS } from '../../entityV2/shared/constants';

export const ExpandContractButton = styled.div<{ expandOnHover?: boolean }>`
    background-color: white;
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 10px;
    color: ${LINEAGE_COLORS.BLUE_1};
    cursor: pointer;
    display: flex;
    font-size: 18px;
    padding: 3px;
    position: absolute;
    top: 50%;

    max-width: 25px;
    overflow: hidden;
    transition: max-width 0.3s ease-in-out;

    :hover {
        ${(props) => props.expandOnHover && `max-width: 50px;`}
    }
`;

export const UpstreamWrapper = styled(ExpandContractButton)`
    right: calc(100% - 5px);
    transform: translateY(-50%) rotate(180deg);
`;

export const DownstreamWrapper = styled(ExpandContractButton)`
    left: calc(100% - 5px);
    transform: translateY(-50%);
`;

export const Button = styled.span`
    border-radius: 20%;
    line-height: 0;

    :hover {
        background-color: ${LINEAGE_COLORS.BLUE_1}30;
    }
`;
