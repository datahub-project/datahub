import React from 'react';
import { Link } from 'react-router-dom';
import { NavMenuItem } from './types';

interface Props {
    menuItem: NavMenuItem;
    key?: string;
}

const CustomNavLink: React.FC<Props> = ({ menuItem: { title, description, link, target, rel, isHidden }, key }) => {
    if (isHidden) {
        return null;
    }

    if (link === null) {
        return <div key={key}>{title}</div>;
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
            {title}
            {description && <span>{description}</span>}
        </LinkComponent>
    );
};

export default CustomNavLink;
