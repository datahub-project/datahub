import { LinkOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { InstitutionalMemoryMetadata } from '../../../../../../types.generated';

export const StyledLink = styled(Button)`
    display: flex;
    align-items: center;
    min-width: 0;
    padding: 0;
    > span:not(.anticon) {
        display: inline-block;
        max-width: 100%;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
    }
`;

interface Props {
    link: InstitutionalMemoryMetadata;
}

export default function LinkButton({ link }: Props) {
    return (
        <StyledLink
            type="link"
            href={link.url}
            target="_blank"
            rel="noreferrer"
            key={`${link.label}-${link.url}-${link.actor.urn}`}
        >
            <LinkOutlined />
            {link.description || link.label}
        </StyledLink>
    );
}
