import { Text } from '@components';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { getPlatformName } from '@src/app/entityV2/shared/utils';
import React from 'react';
import { useStatsSectionsContext } from '../../StatsSectionsContext';

const VIEW_DOCUMENTATION_LINK = 'https://datahubproject.io/integrations/';

const MoreInfoModalContent = () => {
    const { statsEntity } = useStatsSectionsContext();
    const platformName = getPlatformName(statsEntity as GenericEntityProperties);

    return (
        <div>
            <Text color="gray" size="md">
                Turn on profiling for the {platformName} source in order to see stats here.
            </Text>
            <Text color="gray" size="md">
                You can{' '}
                <a href={VIEW_DOCUMENTATION_LINK} target="_blank" rel="noreferrer noopener">
                    view documentation
                </a>{' '}
                for more help.
            </Text>
        </div>
    );
};

export default MoreInfoModalContent;
