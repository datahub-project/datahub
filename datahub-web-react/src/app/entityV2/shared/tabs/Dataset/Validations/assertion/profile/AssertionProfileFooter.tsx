import { BellTwoTone } from '@ant-design/icons';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0px 24px;
    margin-bottom: 20px;
`;

const Tip = styled.div`
    background-color: ${(props) => props.theme.colors.bgSurface};
    padding: 20px;
    border-radius: 4px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const Title = styled.div`
    font-weight: 700;
    margin-bottom: 8px;
`;

const StyledBell = styled(BellTwoTone)`
    margin-right: 4px;
`;

// TODO: Add support for V2 styled actions: Delete, start, stop.
export const AssertionProfileFooter = () => {
    const { t } = useTranslation('entity.profile.validations');
    return (
        <Container>
            <Tip>
                <Title>
                    <StyledBell /> {t('profile.notifyWhenWrong')}
                </Title>
                <Trans
                    t={t}
                    i18nKey="profile.notifyBody"
                    components={{
                        anchor: (
                            // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                            <a
                                href="https://docs.datahub.com/docs/next/managed-datahub/subscription-and-notification/"
                                target="_blank"
                                rel="noreferrer noopener"
                            />
                        ),
                    }}
                />
            </Tip>
        </Container>
    );
};
