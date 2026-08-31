import React from 'react';

import ResetCredentialsModal from '@app/auth/resetCredentialsV2/ResetCredentialsModal';
import AuthPageContainer from '@app/auth/shared/AuthPageContainer';

export default function ResetCredentialsV2() {
    return (
        <AuthPageContainer>
            <ResetCredentialsModal />
        </AuthPageContainer>
    );
}
