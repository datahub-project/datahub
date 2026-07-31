import { Icon } from '@components';
import { House } from '@phosphor-icons/react/dist/csr/House';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Wrapper = styled.div`
    background-color: ${(props) => props.theme.colors.bgSurface};
    border-top-left-radius: 4px;
    border-top-right-radius: 4px;
    border: 1px solid ${(props) => props.theme.colors.border};
    color: ${(p) => p.theme.colors.textBrand};

    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    padding: 0 4px;
`;

const StyledIcon = styled(Icon)`
    margin-left: -1px;
`;

interface Props {
    showText: boolean;
}

export default function HomePill({ showText }: Props) {
    const { t } = useTranslation('lineage');
    return (
        <Wrapper>
            <StyledIcon icon={House} weight="fill" size="lg" />
            {showText && t('node.homePill.label')}
        </Wrapper>
    );
}
