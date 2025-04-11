import { Popover } from '@components';
import { TooltipProps } from 'antd';
import * as React from 'react';
import {
    Container,
    Content,
    Section,
    SectionHeader,
    SectionsContainer,
    SectionTitle,
    Title,
    TitleSuffix,
} from './components';
import { Tooltip2Props } from './types';

/**
 * Note: Depends on styling set in global-overrides-v2.less to override ant padding
 */
export function Tooltip2(props: Tooltip2Props & TooltipProps) {
    const { header: Header, width, title, sections, ...otherProps } = props;

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
                borderRadius: '8px',
            }}
        >
            {props.children}
        </Popover>
    );
}
