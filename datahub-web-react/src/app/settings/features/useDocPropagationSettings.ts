/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { message } from 'antd';
import { useEffect, useState } from 'react';

import { useGetDocPropagationSettingsQuery, useUpdateDocPropagationSettingsMutation } from '@graphql/app.generated';

// Hook to get the document propagation settings & manage state
export const useGetDocPropagationSettings = () => {
    const { data, refetch } = useGetDocPropagationSettingsQuery();
    const [isColPropagateChecked, setIsColPropagateChecked] = useState<boolean>(false);

    useEffect(() => {
        const docPropSetting = data?.docPropagationSettings?.docColumnPropagation;
        if (docPropSetting !== undefined) setIsColPropagateChecked(!!docPropSetting);
    }, [data]);

    return {
        isColPropagateChecked,
        setIsColPropagateChecked,
        refetch,
    };
};

// Hook to update the document propagation settings
export const useUpdateDocPropagationSettings = () => {
    const [updateDocPropagationSettings] = useUpdateDocPropagationSettingsMutation();
    const { refetch } = useGetDocPropagationSettingsQuery();

    const updateDocPropagation = async (checked: boolean) => {
        try {
            await updateDocPropagationSettings({
                variables: {
                    input: {
                        docColumnPropagation: checked,
                    },
                },
            });
            refetch();
            message.success('Successfully updated documentation propagation settings');
        } catch (e) {
            message.error('Failed to update documentation propagation settings');
            refetch();
        }
    };

    return { updateDocPropagation };
};
