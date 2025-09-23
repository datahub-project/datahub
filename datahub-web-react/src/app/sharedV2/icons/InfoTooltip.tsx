import { InfoCircleFilled, InfoCircleOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

const InfoWrapper = styled.div`
    color: ${(props) => props.theme.styles['primary-color']};
`;

interface Props {
    content: React.ReactNode;
    className?: string;
}

export default function InfoTooltip({ content, className }: Props) {
    const [showTooltip, setShowTooltip] = useState(false);

    return (
        <InfoWrapper className={className}>
            <Tooltip
                placement="top"
                title={content}
                trigger="hover"
                open={showTooltip}
                onOpenChange={setShowTooltip}
                showArrow={false}
            >
                {showTooltip ? <InfoCircleFilled /> : <InfoCircleOutlined />}
            </Tooltip>
        </InfoWrapper>
    );
}
