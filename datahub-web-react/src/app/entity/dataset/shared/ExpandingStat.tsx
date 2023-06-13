import React, { ReactNode, useRef, useState } from 'react';
import styled from 'styled-components';

const ExpandingStatContainer = styled.span<{ disabled: boolean; expanded: boolean; color: string }>`
    color: ${(props) => props.color};
    ${(props) =>
        !props.disabled &&
        !props.expanded &&
        `
    :hover {
        color: ${props.theme.styles['primary-color']};
        cursor: pointer;
    }
`}
`;

const ExpandingStat = ({
    color,
    disabled = false,
    render,
}: {
    color: string;
    disabled?: boolean;
    render: (isExpanded: boolean) => ReactNode;
}) => {
    const ref = useRef<HTMLSpanElement>(null);
    const [isExpanded, setIsExpanded] = useState(false);

    const onClickContainer = () => {
        if (!disabled) setIsExpanded(true);
    };

    return (
        <ExpandingStatContainer
            disabled={disabled}
            expanded={isExpanded}
            color={color}
            ref={ref}
            onClick={onClickContainer}
        >
            {render(isExpanded)}
        </ExpandingStatContainer>
    );
};

export default ExpandingStat;
