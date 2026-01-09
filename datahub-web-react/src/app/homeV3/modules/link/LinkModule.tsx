import { Icon, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import SmallModule from '@app/homeV3/module/components/SmallModule';
import { ModuleProps } from '@app/homeV3/module/types';
import ImageOrIcon from '@app/homeV3/modules/link/ImageOrIcon';
import { DescriptionContainer, NameContainer } from '@app/homeV3/styledComponents';

const Container = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-right: 8px;
`;

const RightSection = styled.div`
    display: flex;
`;

const LeftSection = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    max-width: calc(100% - 24px);
`;

const TextSection = styled.div`
    display: flex;
    flex-direction: column;
    max-width: calc(100% - 30px);
`;

export default function LinkModule(props: ModuleProps) {
    const { name } = props.module.properties;
    const { linkParams } = props.module.properties.params;

    function goToLink() {
        if (linkParams?.linkUrl) {
            window.open(linkParams.linkUrl, '_blank');
            analytics.event({
                type: EventType.HomePageTemplateModuleLinkClick,
                link: linkParams.linkUrl,
            });
        }
    }

    return (
        <SmallModule {...props} onClick={goToLink} dataTestId="link-module">
            <Container>
                <LeftSection>
                    <ImageOrIcon imageUrl={linkParams?.imageUrl} />

                    <TextSection>
                        <NameContainer
                            ellipsis={{
                                tooltip: {
                                    color: 'white',
                                    overlayInnerStyle: { color: colors.gray[1700] },
                                    showArrow: false,
                                },
                            }}
                        >
                            {name}
                        </NameContainer>
                        {linkParams?.description && (
                            <DescriptionContainer
                                ellipsis={{
                                    tooltip: {
                                        color: 'white',
                                        overlayInnerStyle: { color: colors.gray[1700] },
                                        showArrow: false,
                                    },
                                }}
                            >
                                {linkParams?.description}
                            </DescriptionContainer>
                        )}
                    </TextSection>
                </LeftSection>
                <RightSection>
                    <a href={linkParams?.linkUrl} target="_blank" rel="noopener noreferrer">
                        <Icon icon="ArrowUpRight" source="phosphor" size="lg" color="gray" />
                    </a>
                </RightSection>
            </Container>
        </SmallModule>
    );
}
