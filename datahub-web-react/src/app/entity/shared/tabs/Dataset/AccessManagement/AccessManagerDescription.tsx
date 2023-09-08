import React, { useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';

export type Props = {
    description: any;
};

const DescriptionContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    width: 500px;
    height: 100%;
    min-height: 22px;
`;

export default function AccessManagerDescription({ description }: Props) {
    const shouldTruncateDescription = description.length > 150;
    const [expanded, setIsExpanded] = useState(!shouldTruncateDescription);
    const finalDescription = expanded ? description : description.slice(0, 150);
    const toggleExpanded = () => {
        setIsExpanded(!expanded);
    };

    return (
        <DescriptionContainer>
            {finalDescription}
            <Typography.Link
                onClick={() => {
                    toggleExpanded();
                }}
            >
                {(shouldTruncateDescription && (expanded ? ' Read Less' : '...Read More')) || undefined}
            </Typography.Link>
        </DescriptionContainer>
    );
}
