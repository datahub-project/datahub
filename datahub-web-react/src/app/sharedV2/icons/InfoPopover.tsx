import { InfoCircleFilled, InfoCircleOutlined } from '@ant-design/icons';
import { Popover } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { getColor } from '@components/theme/utils';

const InfoWrapper = styled.div<{ $iconColor?: string }>`
    color: ${({ theme, $iconColor }) => $iconColor || getColor('primary', 500, theme)};
`;

interface Props {
    content: React.ReactNode;
    className?: string;
    iconColor?: string;
}

export default function InfoPopover({ content, className, iconColor }: Props) {
    const [showPopover, setShowPopover] = useState(false);

    return (
        <InfoWrapper className={className} $iconColor={iconColor}>
            <Popover placement="top" content={content} trigger="hover" open={showPopover} onOpenChange={setShowPopover}>
                {showPopover ? <InfoCircleFilled /> : <InfoCircleOutlined />}
            </Popover>
        </InfoWrapper>
    );
}
