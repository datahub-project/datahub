/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import SectionHeader from '@app/searchV2/searchBarV2/components/SectionHeader';
import useRecentlyViewedEntities from '@app/searchV2/searchBarV2/hooks/useRecentlyViewedEntities';
import { SectionOption } from '@app/searchV2/searchBarV2/types';

export default function useRecentlyViewedEntitiesOptions(): SectionOption[] {
    const { entities: recentlyViewedEntities } = useRecentlyViewedEntities();

    const recentlyViewedEntitiesOptions = useMemo(
        () =>
            recentlyViewedEntities.length > 0
                ? [
                      {
                          label: <SectionHeader text="You Recently Viewed" />,
                          options: recentlyViewedEntities.map((entity) => ({
                              label: (
                                  <AutoCompleteEntityItem
                                      entity={entity}
                                      variant="searchBar"
                                      dataTestId={`recently-viewed-${entity.urn}`}
                                  />
                              ),
                              value: entity.urn,
                              type: entity.type,
                              style: { padding: '0 8px' },
                          })),
                      },
                  ]
                : [],
        [recentlyViewedEntities],
    );

    return recentlyViewedEntitiesOptions;
}
