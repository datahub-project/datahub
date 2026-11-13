import { Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import GithubStarLink from '@app/sharedV2/githubStar/GithubStarLink';
import useGithubStarCount from '@app/sharedV2/githubStar/useGithubStarCount';
import { useAppConfig } from '@app/useAppConfig';

type Props = {
    className?: string;
};

export default function GithubStarButton({ className }: Props) {
    const { t } = useTranslation('shared.misc');
    const { config } = useAppConfig();
    const isEnabled = config.featureFlags.showGithubStarPrompt;
    const starCount = useGithubStarCount(isEnabled);

    if (!isEnabled) return null;

    return (
        <Tooltip title={t('githubStar.tooltip')} placement="bottom">
            <GithubStarLink className={className} starCount={starCount} source="pageHeader" />
        </Tooltip>
    );
}
