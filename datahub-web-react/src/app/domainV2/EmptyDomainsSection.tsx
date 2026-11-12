import { Button, Heading } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

const EmptyDomainContainer = styled.div`
    display: flex;
    justify-content: center;
    overflow-y: auto;
`;

const EmptyContent = styled.div`
    width: 35vw;
    @media screen and (max-width: 1300px) {
        width: 50vw;
    }
    @media screen and (max-width: 896px) {
        overflow-y: auto;
        max-height: 75vh;
        &::-webkit-scrollbar {
            width: 5px;
            background: ${(props) => props.theme.colors.bgSurface};
        }
    }
    padding: 20px;
    margin: 32px 8px;
    display: flex;
    flex-direction: column;
    align-items: center;
    text-align: center;
`;

const ButtonContainer = styled.div`
    margin: 18px 8px 0 0;
`;

const IconContainer = styled.span`
    color: ${(props) => props.theme.colors.icon};
    font-size: 40px;
`;

interface Props {
    title?: string;
    setIsCreatingDomain: React.Dispatch<React.SetStateAction<boolean>>;
    description?: React.ReactNode;
    icon?: React.ReactNode;
}

function EmptyDomainsSection(props: Props) {
    const { title, description, setIsCreatingDomain, icon } = props;
    const { t } = useTranslation('governance.domain');
    return (
        <EmptyDomainContainer>
            <EmptyContent>
                <IconContainer>{icon}</IconContainer>
                <Heading type="h4" size="xl" weight="semiBold">
                    {title}
                </Heading>
                {description}
                <ButtonContainer>
                    <Button variant="outline" icon={{ icon: Plus }} onClick={() => setIsCreatingDomain(true)}>
                        {t('empty.createButton')}
                    </Button>
                </ButtonContainer>
            </EmptyContent>
        </EmptyDomainContainer>
    );
}

export default EmptyDomainsSection;
