import { Icon, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import SmallModule from '@app/homeV3/module/components/SmallModule';
import { ModuleProps } from '@app/homeV3/module/types';

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
`;

const TextSection = styled.div`
    display: flex;
    flex-direction: column;
`;

const Image = styled.img`
    height: 24px;
    width: 24px;
    object-fit: contain;
    background-color: transparent;
`;

export default function LinkModule(props: ModuleProps) {
    const { name } = props.module.properties;
    const { linkParams } = props.module.properties.params;

    function goToLink() {
        if (linkParams?.linkUrl) {
            window.open(linkParams.linkUrl, '_blank');
        }
    }

    return (
        <SmallModule {...props} onClick={goToLink}>
            <Container>
                <LeftSection>
                    {linkParams?.imageUrl ? (
                        <Image src={linkParams?.imageUrl} />
                    ) : (
                        <Icon icon="LinkSimple" source="phosphor" size="3xl" color="gray" />
                    )}
                    <TextSection>
                        <Text color="gray" colorLevel={600} weight="bold" size="lg" lineHeight="normal">
                            {name}
                        </Text>
                        {linkParams?.description && (
                            <Text color="gray" size="sm">
                                {linkParams?.description}
                            </Text>
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
