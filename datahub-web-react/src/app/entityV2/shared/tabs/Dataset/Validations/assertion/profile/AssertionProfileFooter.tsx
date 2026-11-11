import { Alert, Icon } from '@components';
import { Bell } from '@phosphor-icons/react/dist/csr/Bell';
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

// TODO: Add support for V2 styled actions: Delete, start, stop.
export const AssertionProfileFooter = () => {
    const { t } = useTranslation('entity.profile.validations');
    return (
        <Container>
            <Alert
                variant="gray"
                icon={<Icon icon={Bell} size="lg" color="inherit" />}
                title={t('profile.notifyWhenWrong')}
                description={
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
                }
            />
        </Container>
    );
};
