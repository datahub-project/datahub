import React from 'react';

import {
    BreadcrumbButton,
    BreadcrumbItemContainer,
    BreadcrumbLink,
    Wrapper,
} from '@components/components/Breadcrumb/components';
import { BreadcrumbProps } from '@components/components/Breadcrumb/types';
import { Icon } from '@components/components/Icon';
import { Text } from '@components/components/Text';

export const Breadcrumb = ({ items }: BreadcrumbProps) => {
    const defaultSeparator = <Icon icon="CaretRight" source="phosphor" size="sm" />;

    return (
        <Wrapper>
            {items.map((item, index) => {
                const isLast = index === items.length - 1;

                let content;

                if (item.href) {
                    content = (
                        <BreadcrumbLink to={item.href} $isCurrent={item.isCurrent}>
                            {item.label}
                        </BreadcrumbLink>
                    );
                } else if (item.onClick) {
                    content = (
                        <BreadcrumbButton
                            size="sm"
                            color="textTertiary"
                            onClick={item.onClick}
                            $isCurrent={item.isCurrent}
                        >
                            {item.label}
                        </BreadcrumbButton>
                    );
                } else {
                    content = (
                        <Text size="sm" weight="medium" color={item.isCurrent ? undefined : 'textTertiary'}>
                            {item.label}
                        </Text>
                    );
                }

                return (
                    <BreadcrumbItemContainer>
                        {content}
                        {!isLast && <>{item.separator || defaultSeparator}</>}
                    </BreadcrumbItemContainer>
                );
            })}
        </Wrapper>
    );
};
