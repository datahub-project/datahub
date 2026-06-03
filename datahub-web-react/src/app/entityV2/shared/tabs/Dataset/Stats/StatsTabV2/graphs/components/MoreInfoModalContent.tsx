import { Text } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import analytics, { EventType } from '@src/app/analytics';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { getPlatformNameFromEntityData } from '@src/app/entityV2/shared/utils';

const VIEW_DOCUMENTATION_LINK = 'https://datahubproject.io/integrations/';

const MoreInfoModalContent = () => {
    const { t } = useTranslation('entity.profile.stats');
    const { statsEntity } = useStatsSectionsContext();
    const platformName = getPlatformNameFromEntityData(statsEntity as GenericEntityProperties);

    function sendAnalytics() {
        analytics.event({
            type: EventType.ClickViewDocumentation,
            location: 'statsTab',
            link: VIEW_DOCUMENTATION_LINK,
        });
    }

    return (
        <div>
            <Text size="md">{t('moreInfoModal.turnOnProfiling', { platformName })}</Text>
            <Text size="md">
                <Trans
                    t={t}
                    i18nKey="moreInfoModal.viewDocumentation"
                    components={{
                        anchor: (
                            // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                            <a
                                href={VIEW_DOCUMENTATION_LINK}
                                onClick={sendAnalytics}
                                target="_blank"
                                rel="noreferrer noopener"
                            />
                        ),
                    }}
                />
            </Text>
        </div>
    );
};

export default MoreInfoModalContent;
