import { CloseOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

const CloseSpan = styled.span`
    :hover {
        color: black;
        cursor: pointer;
    }
`;

interface Props {
    onClose: () => void;
}

export default function AdvancedFilterCloseButton({ onClose }: Props) {
    return (
        <CloseSpan
            role="button"
            onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                onClose();
            }}
            tabIndex={0}
            onKeyPress={onClose}
        >
            <CloseOutlined />
        </CloseSpan>
    );
}
