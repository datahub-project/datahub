import React from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { PostContent } from '../../../../../types.generated';
import { PinnedLinkLogo } from './PinnedLinkLogo';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

const LinkButton = styled.a`
    padding: 16px;
    height: auto;
    border-radius: 8px;
    display: flex;
    align-items: center;
    line-height: 16px;
    background-color: #fff;
`;

const IconColumn = styled.div`
    margin-right: 4px;
    display: flex;
`;

const TextColumn = styled.div`
    flex: 1;
    display: flex;
    align-items: flex-start;
    flex-direction: column;
    justify-content: center;
    > div {
        overflow: hidden;
        text-overflow: ellipsis;
        text-wrap: nowrap;
        font-size: 14px;
        font-weight: 400;
        max-width: 220px;
    }
`;

const Title = styled.div``;

const Description = styled.div`
    color: ${ANTD_GRAY[7]};
    padding-top: 4px;
`;

type Props = {
    link: PostContent;
};

export const PinnedLink = ({ link }: Props) => {
    if (!link || !link.link) return null;

    const { title } = link;
    const { description } = link;

    return (
        <LinkButton href={link.link} target="_blank" rel="noopener noreferrer">
            <IconColumn>
                <PinnedLinkLogo link={link} />
            </IconColumn>
            <Tooltip showArrow={false} title={description || title} placement="right">
                <TextColumn>
                    <Title>{title}</Title>
                    {description && <Description>{description}</Description>}
                </TextColumn>
            </Tooltip>
        </LinkButton>
    );
};
