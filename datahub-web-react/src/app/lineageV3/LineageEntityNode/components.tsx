import styled from 'styled-components';

const ExpandContractButton = styled.div<{ expandOnHover?: boolean }>`
    background-color: ${(props) => props.theme.colors.bg};
    color: ${(props) => props.theme.colors.iconBrand};
    cursor: pointer;
    font-size: 18px;

    border-radius: 4px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};

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

/**
 * Shared look for the small controls anchored just outside a node's left or right edge, e.g. the
 * expand / contract lineage controls and a column's lineage controls. Consumers position it, as
 * they anchor to different parts of the node.
 */
export const SideControlWrapper = styled.div`
    position: absolute;
    transform: translateY(-50%);

    // Slightly translucent, so edges passing under a control aren't hidden
    background-color: color-mix(in srgb, ${(props) => props.theme.colors.bg} 90%, transparent);
    border-radius: 4px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    color: ${(props) => props.theme.colors.iconBrand};
    font-size: 18px;

    display: flex;
    align-items: center;
`;

/** Counts shown inside a side control, e.g. `3/7`. */
export const CountText = styled.span`
    font-size: 12px;
    font-weight: 600;
    white-space: nowrap;
`;

export const Button = styled.span`
    display: flex;
    align-items: center;
    cursor: pointer;
    border-radius: 4px;
    font-size: 12px;

    line-height: 0;
    padding: 4px;

    :hover {
        background-color: ${(props) => props.theme.colors.bgSurfaceNewNav};
    }
`;
