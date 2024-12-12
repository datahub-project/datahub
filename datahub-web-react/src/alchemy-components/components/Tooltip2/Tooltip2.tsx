import { Tooltip, TooltipProps } from 'antd';
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

export function Tooltip2(props: Tooltip2Props & TooltipProps) {
    const { header: Header, width, title, sections, ...otherProps } = props;

    if (!Header && !title) return null;

    const renderTitle = () => {
        return (
            <Container>
                {Header && <Header />}
                <Title>{title}</Title>
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
    };

    return (
        <Tooltip
            {...otherProps}
            showArrow={false}
            title={renderTitle()}
            overlayStyle={{
                width: `${width || 278}px`,
            }}
            overlayInnerStyle={{
                fontFamily: 'Mulish',
                padding: '12px',
                backgroundColor: 'white',
                borderRadius: '8px',
            }}
        >
            {props.children}
        </Tooltip>
    );
}
