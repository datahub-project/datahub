import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import styled from 'styled-components';

const StyledX = styled(X)`
    color: ${(props) => props.theme.colors.icon};
    cursor: pointer;
    flex-shrink: 0;

    :hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

interface Props {
    onClick?: (e: React.MouseEvent<SVGSVGElement>) => void;
    /** Layout overrides (e.g. `margin-left`) applied via `styled(PillRemoveIcon)` extension. */
    className?: string;
    size?: number;
    dataTestId?: string;
}

/**
 * Phosphor `X` styled as a "remove" affordance inside pills and entity-link chips
 * (tags, glossary terms, domains, data products, applications). Defaults to 12px /
 * regular weight with semantic icon hover colors. Consumers add layout (margin,
 * positioning) via `className` to keep this component layout-agnostic.
 */
export default function PillRemoveIcon({ onClick, className, size = 12, dataTestId = 'remove-icon' }: Props) {
    return <StyledX size={size} weight="regular" onClick={onClick} className={className} data-testid={dataTestId} />;
}
