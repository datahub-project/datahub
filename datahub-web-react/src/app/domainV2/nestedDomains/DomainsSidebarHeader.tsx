import { Button, Tooltip } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import CreateDomainModal from '@app/domainV2/CreateDomainModal';

const Wrapper = styled.div`
    font-size: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
`;

const DomainTitle = styled.div`
    font-size: 16px;
    font-weight: bold;
    color: ${(props) => props.theme.colors.text};
`;

const StyledButton = styled(Button)`
    padding: 2px;
    margin-right: 4px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

export default function DomainsSidebarHeader() {
    const { t } = useTranslation('governance.domain');
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);

    return (
        <Wrapper>
            <DomainTitle>{t('page.title')}</DomainTitle>
            <Tooltip showArrow={false} title={t('sidebar.createTooltip')} placement="right">
                <StyledButton
                    variant="filled"
                    color="violet"
                    isCircle
                    icon={{ icon: Plus }}
                    onClick={() => setIsCreatingDomain(true)}
                    data-testid="sidebar-create-domain-button"
                />
            </Tooltip>
            {isCreatingDomain && (
                <CreateDomainModal
                    onClose={() => setIsCreatingDomain(false)}
                    onCreate={() => setIsCreatingDomain(false)}
                />
            )}
        </Wrapper>
    );
}
