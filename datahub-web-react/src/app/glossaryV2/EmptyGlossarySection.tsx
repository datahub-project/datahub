import { Button, Heading, Text } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Empty } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

const StyledEmpty = styled(Empty)`
    padding: 80px 40px;
    .ant-empty-footer {
        .ant-btn:not(:last-child) {
            margin-right: 8px;
        }
    }
`;

const StyledButton = styled(Button)`
    margin-right: 8px;
`;

interface Props {
    title?: string;
    description?: string;
    onAddTerm: () => void;
    onAddtermGroup: () => void;
}

function EmptyGlossarySection(props: Props) {
    const { t } = useTranslation('governance.glossary');
    const { title, description, onAddTerm, onAddtermGroup } = props;

    return (
        <>
            <StyledEmpty
                description={
                    <>
                        <Heading type="h4">{title}</Heading>
                        <Text color="textSecondary">{description}</Text>
                    </>
                }
            >
                {/* not disabled on acryl-main due to ability to propose */}
                <StyledButton data-testid="add-term-button" icon={{ icon: Plus }} onClick={onAddTerm}>
                    {t('empty.addTerm')}
                </StyledButton>
                <StyledButton data-testid="add-term-group-button" icon={{ icon: Plus }} onClick={onAddtermGroup}>
                    {t('empty.addTermGroup')}
                </StyledButton>
            </StyledEmpty>
        </>
    );
}

export default EmptyGlossarySection;
