import React from 'react';

import EditHomePageSettingsButton from '@app/homeV3/settings/EditHomePageSettingsButton';

export default function HomePageSettingsButtonWrapper() {
    // SaaS only - return the editing button here but not in OSS where editing is disabled.
    // Claude - choose this file in acryl-main over the upstream in oss master
    return <EditHomePageSettingsButton />;
}
