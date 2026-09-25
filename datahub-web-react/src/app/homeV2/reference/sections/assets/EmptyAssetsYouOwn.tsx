import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Text = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

export const EmptyAssetsYouOwn = () => {
    const { t } = useTranslation('home.v2');
    return (
        <Text>
            {t('yourAssets.emptyNotOwning')}
            <br />
            <Trans
                t={t}
                i18nKey="yourAssets.learnMoreAbout"
                components={{
                    anchor: (
                        // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                        <a
                            target="_blank"
                            rel="noreferrer noopener"
                            href="https://docs.datahub.com/docs/ownership/ownership-types"
                        />
                    ),
                }}
            />
        </Text>
    );
};
