import styled from 'styled-components';

export const ExpandContractButton = styled.div<{ expandOnHover?: boolean }>`
    background-color: ${(props) => props.theme.colors.bgSurface};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 10px;
    color: ${(props) => props.theme.colors.textBrand};
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
        background-color: ${(props) => props.theme.colors.bgSurfaceBrand};
    }
`;
