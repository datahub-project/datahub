import React, { ReactNode, useState } from 'react';
import styled from 'styled-components';

const ExpandingStatContainer = styled.span<{ disabled: boolean; expanded: boolean; color: string }>`
    color: ${(props) => props.color};
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
    const [isExpanded, setIsExpanded] = useState(false);

    const onMouseEnter = () => {
        if (!disabled) setIsExpanded(true);
    };

    const onMouseLeave = () => {
        if (!disabled) setIsExpanded(false);
    };

    return (
        <ExpandingStatContainer
            disabled={disabled}
            expanded={isExpanded}
            color={color}
            onMouseEnter={onMouseEnter}
            onMouseLeave={onMouseLeave}
        >
            {render(isExpanded)}
        </ExpandingStatContainer>
    );
};

export default ExpandingStat;
