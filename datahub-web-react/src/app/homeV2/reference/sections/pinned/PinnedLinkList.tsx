import React from 'react';
import styled from 'styled-components';
import { Carousel } from '@src/app/sharedV2/carousel/Carousel';
import { Tooltip } from '@components';
import { PinnedLink } from './PinnedLink';

const Title = styled.div<{ hasAction: boolean }>`
    ${(props) => props.hasAction && `:hover { cursor: pointer; }`}
    margin-bottom: 8px;
    color: #403d5c;
    font-weight: 600;
    font-size: 16px;
`;

type Props = {
    title?: string;
    tip?: React.ReactNode;
    links: any[];
    onClickTitle?: () => void;
};

export const PinnedLinkList = ({ title, tip, links, onClickTitle }: Props) => {
    return (
        <>
            {title ? (
                <Title hasAction={onClickTitle !== undefined} onClick={onClickTitle}>
                    <Tooltip title={tip} showArrow={false} placement="right">
                        {title}
                    </Tooltip>
                </Title>
            ) : null}
            <Carousel>
                {links.map((link) => {
                    return <PinnedLink key={`${title}-${link.link}`} link={link} />;
                })}
            </Carousel>
        </>
    );
};
