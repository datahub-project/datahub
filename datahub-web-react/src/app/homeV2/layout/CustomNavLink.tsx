import { Pill } from '@src/alchemy-components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { NavMenuItem } from './types';

const OptionContainer = styled.div``;

const LinkTitle = styled.span`
    display: block;
    color: #fff;
    font: 700 12px/20px Mulish;
    white-space: break-spaces;
`;

const TitleWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const DescriptionWrapper = styled.span`
    display: block;
    color: #fff;
    white-space: break-spaces;

    display: block;
    font: 600 10px/12px Mulish;
`;

const PillWrapper = styled.span`
    margin: 4px 0 4px 6px;
`;

interface Props {
    menuItem: NavMenuItem;
    key: string;
}

const CustomNavLink: React.FC<Props> = ({
    menuItem: { title, showNewTag, description, link, target, rel, isHidden, onClick },
    key,
}) => {
    if (isHidden) {
        return null;
    }

    if (link === null) {
        return <LinkTitle key={key}>{title}</LinkTitle>;
    }

    if (onClick) {
        return (
            <OptionContainer onClick={onClick} key={key}>
                <TitleWrapper>
                    <LinkTitle>{title}</LinkTitle>
                </TitleWrapper>
                {description && <DescriptionWrapper>{description}</DescriptionWrapper>}
            </OptionContainer>
        );
    }

    const isExternalLink = target === '_blank';

    const linkProps = {
        to: isExternalLink ? undefined : link,
        href: isExternalLink ? link : undefined,
        target: isExternalLink ? '_blank' : undefined,
        rel: isExternalLink ? 'noopener noreferrer' : rel,
        'aria-label': title,
        key,
    };

    const LinkComponent = linkProps.target === '_blank' ? 'a' : Link;

    return (
        <LinkComponent {...linkProps}>
            <TitleWrapper>
                <LinkTitle>{title}</LinkTitle>
                {showNewTag && (
                    <PillWrapper>
                        <Pill label="New" size="xs" clickable={false} color="blue" />
                    </PillWrapper>
                )}
            </TitleWrapper>
            {description && <DescriptionWrapper>{description}</DescriptionWrapper>}
        </LinkComponent>
    );
};

export default CustomNavLink;
