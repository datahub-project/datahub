import { InfoCircleFilled, InfoCircleOutlined } from '@ant-design/icons';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

const InfoWrapper = styled.div`
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
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
