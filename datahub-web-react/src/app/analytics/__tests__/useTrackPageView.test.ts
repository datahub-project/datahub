import { describe, expect, it } from 'vitest';

import { isSameEntityTabNavigation } from '@app/analytics/useTrackPageView';

describe('isSameEntityTabNavigation', () => {
    // Test data using actual entity types
    const sampleUrn = 'urn:li:dataset:(urn:li:dataPlatform:mysql,my_database.my_table,PROD)';
    const encodedUrn = encodeURIComponent(sampleUrn);
    const differentUrn = 'urn:li:dataset:(urn:li:dataPlatform:postgres,other_db.other_table,PROD)';
    const encodedDifferentUrn = encodeURIComponent(differentUrn);

    describe('same entity, different tabs', () => {
        it('should return true when navigating between tabs of the same dataset', () => {
            const prevPath = `/dataset/${encodedUrn}/Schema`;
            const currentPath = `/dataset/${encodedUrn}/Properties`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
        });

        it('should return true when navigating between tabs of the same dashboard', () => {
            const dashboardUrn = 'urn:li:dashboard:(looker,my_dashboard)';
            const encodedDashboardUrn = encodeURIComponent(dashboardUrn);
            const prevPath = `/dashboard/${encodedDashboardUrn}/Charts`;
            const currentPath = `/dashboard/${encodedDashboardUrn}/Properties`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
        });

        it('should return true when navigating to entity root from a tab', () => {
            const prevPath = `/dataset/${encodedUrn}/Schema`;
            const currentPath = `/dataset/${encodedUrn}/Properties`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
        });

        it('should return true with additional path segments', () => {
            const prevPath = `/dataset/${encodedUrn}/Schema/field/my_field`;
            const currentPath = `/dataset/${encodedUrn}/Properties/owners`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
        });

        it('should return true with query parameters', () => {
            const prevPath = `/dataset/${encodedUrn}/Schema?view=table`;
            const currentPath = `/dataset/${encodedUrn}/Properties?mode=edit`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
        });
    });

    describe('different entities', () => {
        it('should return false when navigating between different datasets', () => {
            const prevPath = `/dataset/${encodedUrn}/Schema`;
            const currentPath = `/dataset/${encodedDifferentUrn}/Schema`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });

        it('should return false when navigating between different entity types', () => {
            const prevPath = `/dataset/${encodedUrn}/Schema`;
            const currentPath = `/dashboard/${encodedUrn}/Properties`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });

        it('should return false when navigating from entity to completely different page', () => {
            const prevPath = `/dataset/${encodedUrn}/Schema`;
            const currentPath = '/search/datasets';

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });
    });

    describe('invalid or non-entity paths', () => {
        it('should return false for root path', () => {
            const prevPath = '/';
            const currentPath = `/dataset/${encodedUrn}/Schema`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });

        it('should return false for search results', () => {
            const prevPath = '/search/datasets';
            const currentPath = '/search/dashboards';

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });

        it('should return false for browse paths', () => {
            const prevPath = '/browse/dataset';
            const currentPath = '/browse/dashboard';

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });

        it('should return false when one path is too short', () => {
            const prevPath = '/dataset';
            const currentPath = `/dataset/${encodedUrn}/Schema`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });

        it('should return false when both paths are too short', () => {
            const prevPath = '/dataset';
            const currentPath = '/dashboard';

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });

        it('should return false for empty paths', () => {
            expect(isSameEntityTabNavigation('', '')).toBe(false);
        });

        it('should return false when one path is empty', () => {
            const currentPath = `/dataset/${encodedUrn}/Schema`;
            expect(isSameEntityTabNavigation('', currentPath)).toBe(false);
            expect(isSameEntityTabNavigation(currentPath, '')).toBe(false);
        });
    });

    describe('edge cases', () => {
        it('should handle URNs with special characters', () => {
            const specialUrn = 'urn:li:dataset:(urn:li:dataPlatform:mysql,my-db.my_table_2023,PROD)';
            const encodedSpecialUrn = encodeURIComponent(specialUrn);
            const prevPath = `/dataset/${encodedSpecialUrn}/Schema`;
            const currentPath = `/dataset/${encodedSpecialUrn}/Properties`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
        });

        it('should handle long URNs', () => {
            const longUrn =
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,very_long_database_name.very_long_schema_name.very_long_table_name_with_lots_of_details,PROD)';
            const encodedLongUrn = encodeURIComponent(longUrn);
            const prevPath = `/dataset/${encodedLongUrn}/Schema`;
            const currentPath = `/dataset/${encodedLongUrn}/Lineage`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
        });

        it('should be case sensitive for entity types', () => {
            const prevPath = `/Dataset/${encodedUrn}/Schema`;
            const currentPath = `/dataset/${encodedUrn}/Properties`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });

        it('should be case sensitive for URNs', () => {
            const upperUrn = sampleUrn.toUpperCase();
            const encodedUpperUrn = encodeURIComponent(upperUrn);
            const prevPath = `/dataset/${encodedUrn}/Schema`;
            const currentPath = `/dataset/${encodedUpperUrn}/Properties`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
        });

        it('should handle paths with trailing slashes', () => {
            const prevPath = `/dataset/${encodedUrn}/Schema/`;
            const currentPath = `/dataset/${encodedUrn}/Properties/`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
        });

        it('should handle malformed URNs', () => {
            const malformedUrn = 'not-a-valid-urn';
            const prevPath = `/dataset/${malformedUrn}/Schema`;
            const currentPath = `/dataset/${malformedUrn}/Properties`;

            expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
        });
    });

    describe('real-world entity types', () => {
        // Test with various actual entity types from the DataHub system
        const testCases = [
            { type: 'dataset', urn: 'urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)' },
            { type: 'dashboard', urn: 'urn:li:dashboard:(looker,my_dashboard)' },
            { type: 'chart', urn: 'urn:li:chart:(looker,my_chart)' },
            { type: 'pipeline', urn: 'urn:li:dataFlow:(airflow,dag_id,cluster)' },
            { type: 'task', urn: 'urn:li:dataJob:(airflow,dag_id,task_id)' },
            { type: 'glossaryTerm', urn: 'urn:li:glossaryTerm:my_term' },
            { type: 'domain', urn: 'urn:li:domain:my_domain' },
            { type: 'container', urn: 'urn:li:container:my_container' },
            { type: 'user', urn: 'urn:li:corpuser:john.doe' },
            { type: 'group', urn: 'urn:li:corpGroup:data_team' },
        ];

        testCases.forEach(({ type, urn }) => {
            it(`should correctly handle ${type} entity navigation`, () => {
                const encodedTestUrn = encodeURIComponent(urn);
                const prevPath = `/${type}/${encodedTestUrn}/Properties`;
                const currentPath = `/${type}/${encodedTestUrn}/Lineage`;

                expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
            });

            it(`should detect different ${type} entities`, () => {
                const encodedTestUrn = encodeURIComponent(urn);
                const differentTestUrn = encodeURIComponent(`${urn}_different`);
                const prevPath = `/${type}/${encodedTestUrn}/Properties`;
                const currentPath = `/${type}/${differentTestUrn}/Properties`;

                expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(false);
            });
        });
    });

    describe('common tab names', () => {
        const commonTabs = [
            'Properties',
            'Schema',
            'Lineage',
            'Documentation',
            'Queries',
            'Stats',
            'Validations',
            'Incidents',
            'Ownership',
            'Glossary Terms',
            'Tags',
            'Domains',
        ];

        commonTabs.forEach((fromTab) => {
            commonTabs.forEach((toTab) => {
                if (fromTab !== toTab) {
                    it(`should return true when navigating from ${fromTab} to ${toTab}`, () => {
                        const prevPath = `/dataset/${encodedUrn}/${fromTab}`;
                        const currentPath = `/dataset/${encodedUrn}/${toTab}`;

                        expect(isSameEntityTabNavigation(prevPath, currentPath)).toBe(true);
                    });
                }
            });
        });
    });
});
