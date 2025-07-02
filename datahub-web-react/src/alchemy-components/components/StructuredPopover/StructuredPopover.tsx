import { Popover } from '@components';
import { TooltipProps } from 'antd';
import * as React from 'react';

import {
    Container,
    Content,
    Section,
    SectionHeader,
    SectionTitle,
    SectionsContainer,
    Title,
    TitleSuffix,
} from '@components/components/StructuredPopover/components';
import { StructuredPopoverProps } from '@components/components/StructuredPopover/types';

/**
 * Note: Depends on styling set in global-overrides-v2.less to override ant padding
 */
export function StructuredPopover(props: StructuredPopoverProps & TooltipProps) {
    const { header: Header, width, title, sections, ...otherProps } = props;

    if (!title && !sections?.length) return <>{props.children}</>;

    const content = (
        <Container>
            {Header && <Header />}
            {title && <Title>{title}</Title>}
            {sections && (
                <SectionsContainer>
                    {sections?.map((section) => (
                        <Section>
                            <SectionHeader>
                                <SectionTitle>{section.title}</SectionTitle>
                                {section.titleSuffix && <TitleSuffix>{section.titleSuffix}</TitleSuffix>}
                            </SectionHeader>
                            {section?.content && <Content>{section.content}</Content>}
                        </Section>
                    ))}
                </SectionsContainer>
            )}
        </Container>
    );

    return (
        <Popover
            {...otherProps}
            content={content}
            overlayClassName="sectioned-tooltip"
            overlayStyle={{
                width: `${width}px`,
            }}
            overlayInnerStyle={{
                fontFamily: 'Mulish',
                padding: '12px',
                borderRadius: '12px',
            }}
        >
            {props.children}
        </Popover>
    );
}
