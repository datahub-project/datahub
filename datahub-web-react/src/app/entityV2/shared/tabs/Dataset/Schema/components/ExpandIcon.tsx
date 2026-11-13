import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import styled, { css } from 'styled-components';

const IconContainer = styled.div`
    vertical-align: middle;
    display: inline-flex;
    gap: 5px;
`;

const caretStyles = css`
    :hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
    color: ${(props) => props.theme.colors.icon};
    padding-right: 5px;
    cursor: pointer;
`;

const Down = styled(CaretDown).attrs({ size: 8, weight: 'bold' as const })`
    ${caretStyles}
`;

const Right = styled(CaretRight).attrs({ size: 8, weight: 'bold' as const })`
    ${caretStyles}
`;

type Props = {
    expanded: boolean;
    onExpand: any;
    expandable: boolean;
    record: any;
};

/** Expand caret for the antd properties table. */
export default function ExpandIcon({ expanded, onExpand, expandable, record }: Props) {
    function toggleExpand(e: React.MouseEvent) {
        e.stopPropagation();
        onExpand(record, e);
    }

    return (
        <IconContainer className="row-icon-container">
            {expandable &&
                record.children !== undefined &&
                (expanded ? (
                    <Down onClick={toggleExpand} data-testid="schema-expand-icon-down" />
                ) : (
                    <Right onClick={toggleExpand} data-testid="schema-expand-icon-right" />
                ))}
        </IconContainer>
    );
}
