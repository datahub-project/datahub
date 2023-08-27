import React, { useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';

export type Props = {
    description: any;
};

export default function AccessManagerDescription({ description }: Props) {
    const DescriptionContainer = styled.div`
        position: relative;
        display: flex;
        flex-direction: column;
        width: 500px;
        height: 100%;
        min-height: 22px;
    `;

    const [expanded, setIsExpanded] = useState(false);
    const toggleExpanded = () => {
        setIsExpanded(!expanded);
    };

    if (description.length > 150) {
        return (
            <DescriptionContainer>
                {expanded ? description : description.slice(0, 150)}
                <Typography.Link
                    onClick={() => {
                        toggleExpanded();
                    }}
                >
                    {expanded ? ' Read Less' : '...Read More'}
                </Typography.Link>
            </DescriptionContainer>
        );
    }
    return <DescriptionContainer>{description}</DescriptionContainer>;
}
