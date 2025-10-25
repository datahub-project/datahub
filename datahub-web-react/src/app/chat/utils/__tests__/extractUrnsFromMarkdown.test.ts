import { extractReferencesFromMarkdown, extractUrnsFromMarkdown } from '@app/chat/utils/extractUrnsFromMarkdown';

describe('extractUrnsFromMarkdown', () => {
    describe('extractReferencesFromMarkdown', () => {
        it('should return empty array for empty or null input', () => {
            expect(extractReferencesFromMarkdown('')).toEqual([]);
            expect(extractReferencesFromMarkdown(null as any)).toEqual([]);
            expect(extractReferencesFromMarkdown(undefined as any)).toEqual([]);
        });

        it('should extract simple URNs from markdown links', () => {
            const markdown = '[Dataset](urn:li:dataset:simple_name)';
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                text: 'Dataset',
                urn: 'urn:li:dataset:simple_name',
            });
        });

        it('should extract URNs with parentheses from markdown links', () => {
            const markdown = '[Dataset](urn:li:dataset:(platform,name,env))';
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                text: 'Dataset',
                urn: 'urn:li:dataset:(platform,name,env)',
            });
        });

        it('should extract nested URNs from markdown links', () => {
            const markdown = '[Dataset](urn:li:dataset:(urn:li:dataPlatform:hive,table_name,PROD))';
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                text: 'Dataset',
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table_name,PROD)',
            });
        });

        it('should extract deeply nested URNs from markdown links', () => {
            const markdown = '[DataJob](urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_name,PROD),task_123))';
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                text: 'DataJob',
                urn: 'urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_name,PROD),task_123)',
            });
        });

        it('should extract URNs from full URLs', () => {
            const markdown =
                '[Dataset](http://localhost:9002/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,table_name,PROD))';
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                text: 'Dataset',
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table_name,PROD)',
            });
        });

        it('should handle URL-encoded URNs', () => {
            const markdown =
                '[Dataset](http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Ctable_name%2CPROD%29)';
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                text: 'Dataset',
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table_name,PROD)',
            });
        });

        it('should extract multiple URNs from multiple markdown links', () => {
            const markdown = `
                [Dataset1](urn:li:dataset:(urn:li:dataPlatform:hive,table1,PROD))
                [Dataset2](urn:li:dataset:(urn:li:dataPlatform:s3,table2,PROD))
                [DataJob](urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),task_123))
            `;
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(3);
            expect(result[0]).toEqual({
                text: 'Dataset1',
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table1,PROD)',
            });
            expect(result[1]).toEqual({
                text: 'Dataset2',
                urn: 'urn:li:dataset:(urn:li:dataPlatform:s3,table2,PROD)',
            });
            expect(result[2]).toEqual({
                text: 'DataJob',
                urn: 'urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),task_123)',
            });
        });

        it('should handle different entity types', () => {
            const markdown = `
                [Dataset](urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD))
                [Chart](urn:li:chart:(urn:li:dataPlatform:looker,chart_id,PROD))
                [Dashboard](urn:li:dashboard:(urn:li:dataPlatform:looker,dashboard_id,PROD))
                [DataJob](urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),task))
                [DataFlow](urn:li:dataFlow:(airflow,dag,PROD))
                [Container](urn:li:container:(urn:li:dataPlatform:kafka,container,PROD))
                [Domain](urn:li:domain:domain_id)
                [DataProduct](urn:li:dataProduct:(urn:li:dataPlatform:datahub,product,PROD))
                [GlossaryTerm](urn:li:glossaryTerm:term_id)
                [GlossaryNode](urn:li:glossaryNode:node_id)
                [Tag](urn:li:tag:tag_id)
                [User](urn:li:corpuser:user_id)
                [Group](urn:li:corpGroup:group_id)
            `;
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(13);
            expect(result.map((r) => r.urn)).toEqual([
                'urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)',
                'urn:li:chart:(urn:li:dataPlatform:looker,chart_id,PROD)',
                'urn:li:dashboard:(urn:li:dataPlatform:looker,dashboard_id,PROD)',
                'urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),task)',
                'urn:li:dataFlow:(airflow,dag,PROD)',
                'urn:li:container:(urn:li:dataPlatform:kafka,container,PROD)',
                'urn:li:domain:domain_id',
                'urn:li:dataProduct:(urn:li:dataPlatform:datahub,product,PROD)',
                'urn:li:glossaryTerm:term_id',
                'urn:li:glossaryNode:node_id',
                'urn:li:tag:tag_id',
                'urn:li:corpuser:user_id',
                'urn:li:corpGroup:group_id',
            ]);
        });

        it('should remove duplicate URNs', () => {
            const markdown = `
                [Dataset1](urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD))
                [Dataset2](urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD))
                [Dataset3](urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD))
            `;
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                text: 'Dataset1',
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)',
            });
        });

        it('should handle complex real-world example', () => {
            const markdown = `I found several logging-related data assets in your catalog:

**Primary Logging Dataset:**
- [logging_events](http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Clogging_events%2CPROD%29/) (Hive): Table where each row represents a single log event.

**Backup Dataset:**
- [logging_events_bckp](http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3As3%2Cproject%2Froot%2Fevents%2Flogging_events_bckp%2CPROD%29/) (S3): S3 backup of the logging events.

**Derived Datasets:**
- [fct_users_created](http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Cfct_users_created%2CPROD%29/) (Hive): Table containing all users created.
- [fct_users_deleted](http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Cfct_users_deleted%2CPROD%29/) (Hive): Table containing all users deleted.

**Data Jobs:**
- [User Creations](http://localhost:9002/tasks/urn%3Ali%3AdataJob%3A%28urn%3Ali%3AdataFlow%3A%28airflow%2Cdag_abc%2CPROD%29%2Ctask_123%29/) (Airflow): Constructs the fct_users_created table.
- [User Deletions](http://localhost:9002/tasks/urn%3Ali%3AdataFlow%3A%28airflow%2Cdag_abc%2CPROD%29%2Ctask_456%29/) (Airflow): Constructs the fct_users_deleted table.`;

            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(6);
            expect(result.map((r) => r.text)).toEqual([
                'logging_events',
                'logging_events_bckp',
                'fct_users_created',
                'fct_users_deleted',
                'User Creations',
                'User Deletions',
            ]);
            expect(result.map((r) => r.urn)).toEqual([
                'urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)',
                'urn:li:dataset:(urn:li:dataPlatform:s3,project/root/events/logging_events_bckp,PROD)',
                'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)',
                'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)',
                'urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_123)',
                'urn:li:dataFlow:(airflow,dag_abc,PROD)',
            ]);
        });

        it('should ignore non-URN markdown links', () => {
            const markdown = `
                [Regular Link](https://example.com)
                [Dataset](urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD))
                [Another Link](http://localhost:3000/some-page)
            `;
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                text: 'Dataset',
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)',
            });
        });

        it('should handle malformed URLs gracefully', () => {
            const markdown = '[Dataset](urn:li:dataset:malformed)';
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                text: 'Dataset',
                urn: 'urn:li:dataset:malformed',
            });
        });

        it('should handle URLs with invalid encoding gracefully', () => {
            const markdown = '[Dataset](http://localhost:9002/dataset/invalid%encoding)';
            const result = extractReferencesFromMarkdown(markdown);

            expect(result).toHaveLength(0);
        });
    });

    describe('extractUrnsFromMarkdown', () => {
        it('should return empty array for empty input', () => {
            expect(extractUrnsFromMarkdown('')).toEqual([]);
            expect(extractUrnsFromMarkdown(null as any)).toEqual([]);
        });

        it('should extract URNs from markdown', () => {
            const markdown = `
                [Dataset1](urn:li:dataset:(urn:li:dataPlatform:hive,table1,PROD))
                [Dataset2](urn:li:dataset:(urn:li:dataPlatform:s3,table2,PROD))
            `;
            const result = extractUrnsFromMarkdown(markdown);

            expect(result).toEqual([
                'urn:li:dataset:(urn:li:dataPlatform:hive,table1,PROD)',
                'urn:li:dataset:(urn:li:dataPlatform:s3,table2,PROD)',
            ]);
        });

        it('should remove duplicate URNs', () => {
            const markdown = `
                [Dataset1](urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD))
                [Dataset2](urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD))
            `;
            const result = extractUrnsFromMarkdown(markdown);

            expect(result).toEqual(['urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)']);
        });

        it('should handle complex real-world example', () => {
            const markdown = `I found several logging-related data assets in your catalog:

**Primary Logging Dataset:**
- [logging_events](http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Clogging_events%2CPROD%29/) (Hive): Table where each row represents a single log event.

**Backup Dataset:**
- [logging_events_bckp](http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3As3%2Cproject%2Froot%2Fevents%2Flogging_events_bckp%2CPROD%29/) (S3): S3 backup of the logging events.

**Derived Datasets:**
- [fct_users_created](http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Cfct_users_created%2CPROD%29/) (Hive): Table containing all users created.
- [fct_users_deleted](http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Cfct_users_deleted%2CPROD%29/) (Hive): Table containing all users deleted.

**Data Jobs:**
- [User Creations](http://localhost:9002/tasks/urn%3Ali%3AdataJob%3A%28urn%3Ali%3AdataFlow%3A%28airflow%2Cdag_abc%2CPROD%29%2Ctask_123%29/) (Airflow): Constructs the fct_users_created table.
- [User Deletions](http://localhost:9002/tasks/urn%3Ali%3AdataFlow%3A%28airflow%2Cdag_abc%2CPROD%29%2Ctask_456%29/) (Airflow): Constructs the fct_users_deleted table.`;

            const result = extractUrnsFromMarkdown(markdown);

            expect(result).toEqual([
                'urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)',
                'urn:li:dataset:(urn:li:dataPlatform:s3,project/root/events/logging_events_bckp,PROD)',
                'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)',
                'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)',
                'urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_123)',
                'urn:li:dataFlow:(airflow,dag_abc,PROD)',
            ]);
        });
    });
});
