import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { PinnedLinkLogo } from '@app/homeV2/reference/sections/pinned/PinnedLinkLogo';
import { SEARCH_COLORS } from '@src/app/entityV2/shared/constants';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { PostContent } from '@types';

const LinkButton = styled.a<{ $isShowNavBarRedesign?: boolean }>`
    padding: 16px;
    height: auto;
    border-radius: 8px;
    display: flex;
    align-items: center;
    line-height: 16px;
    background-color: ${(props) => props.theme.colors.bg};

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        border: 1px solid ${props.theme.colors.border};
        border-radius: 8px;

        :hover {
            border: 1px solid ${SEARCH_COLORS.LINK_BLUE};
        }
    `}
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
    color: ${(props) => props.theme.colors.textTertiary};
    padding-top: 4px;
`;

type Props = {
    link: PostContent;
};

export const PinnedLink = ({ link }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    if (!link || !link.link) return null;

    const { title } = link;
    const { description } = link;

    return (
        <LinkButton
            href={link.link}
            target="_blank"
            rel="noopener noreferrer"
            $isShowNavBarRedesign={isShowNavBarRedesign}
        >
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
