import React from 'react';

import LoginModal from '@app/auth/loginV2/LoginModal';
import AuthPageContainer from '@app/auth/shared/AuthPageContainer';

export default function LoginV2() {
    return (
        <AuthPageContainer>
            <LoginModal />
        </AuthPageContainer>
    );
}
