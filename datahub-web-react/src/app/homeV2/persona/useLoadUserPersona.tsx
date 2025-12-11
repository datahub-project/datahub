/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useUserContext } from '@app/context/useUserContext';
import { PersonaType } from '@app/homeV2/shared/types';

export const useLoadUserPersona = () => {
    const user = useUserContext();
    const userUrn = user.user?.urn;
    if (!userUrn) {
        return { persona: PersonaType.BUSINESS_USER, role: undefined };
    }
    const persona = user.user?.editableProperties?.persona?.urn;
    const title = user.user?.editableProperties?.title;
    return {
        persona,
        title,
    };
};
