import { Icon, Tooltip } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React, { useState } from 'react';
import styled from 'styled-components';

const InfoWrapper = styled.div`
    color: ${(props) => props.theme.colors.textBrand};
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
                <Icon icon={Info} size="md" weight={showTooltip ? 'fill' : 'regular'} color="inherit" />
            </Tooltip>
        </InfoWrapper>
    );
}
