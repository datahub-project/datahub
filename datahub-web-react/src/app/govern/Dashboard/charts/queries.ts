// Query strings for the charts in the Governance Dashboard
export const sqlQueries = (
    daysSinceDate: string,
    formId?: string,
    assigneeId?: string,
    domainId?: string,
    snapshotDate?: string,
    tab?: string,
    series?: number,
) => {
    // Define our entity type based on tab selection
    let entity;
    if (tab === 'byForm') entity = 'form';
    if (tab === 'byAssignee') entity = 'assignee';
    if (tab === 'byDomain') entity = 'domain';

    // Builds the query based on the entity
    // Reduces code duplication
    const queryBuilder = (query: string) => {
        let updatedQuery = query;

        const injectWhere = (whereItem: string) => updatedQuery.replaceAll('where', `where ${whereItem} and `);

        if (entity === 'form') updatedQuery = injectWhere(`form_urn = '${formId}'`);
        if (entity === 'assignee') updatedQuery = injectWhere(`assignee_urn = '${assigneeId}'`);
        if (entity === 'domain') {
            if (domainId === null || domainId === 'null' || domainId === '') {
                updatedQuery = injectWhere('domain_urn is null');
            } else {
                updatedQuery = injectWhere(`domain_urn = '${domainId}'`);
            }
        }

        return updatedQuery;
    };

    // Date truncation
    const dateTrunc = () => {
        let trunc = 'day'; // last 7 days
        if (series === 30) trunc = 'day'; // last 30 days
        if (series === 90) trunc = 'week'; // last 90 days
        if (series === 365) trunc = 'month'; // last 365 days
        if (series === 10000) trunc = 'month'; // last 365 days
        return trunc;
    };

    // assets are complete or not started only if all of their forms are either complete or not_started
    const completeOrNotStartedCount = (status: string) => `
		COUNT(DISTINCT 
			CASE 
				WHEN form_status = '${status}' 
				AND asset_urn NOT IN (
					SELECT DISTINCT asset_urn 
					FROM '{{ table }}' 
					where form_status != '${status}'
				)
				THEN asset_urn 
			END
		)
	`;

    const completeOrNotStartedPercentAndCount = (status: string) => `
		${completeOrNotStartedCount(status)} / count(distinct(asset_urn)) as completed_asset_percent,
		${completeOrNotStartedCount(status)} as completed_asset_count,
		count(distinct(asset_urn)) as assigned_asset_count
	`;

    // assets are in progress if they have one or more forms in_progress OR they have forms in both complete AND not_started
    const inProgressCount = `
		COUNT(DISTINCT 
			CASE 
				WHEN form_status = 'in_progress' 
				OR (asset_urn IN (
					SELECT DISTINCT asset_urn 
					FROM '{{ table }}' 
					where form_status = 'complete'
					)
				AND asset_urn IN (
					SELECT DISTINCT asset_urn 
					FROM '{{ table }}' 
					where form_status = 'not_started'
				))
				THEN asset_urn 
			END
		)
	`;

    const inProgressPercentAndCount = `
		${inProgressCount} / count(distinct(asset_urn)) as completed_asset_percent,
		${inProgressCount} as completed_asset_count,
		count(distinct(asset_urn)) as assigned_asset_count
	`;

    // Reusable select for trend stats
    const trend = `
		form_assigned_date as date,
		count(distinct(asset_urn)) as value
	`;

    // Reusable select aggregate for status categories - only to be used BY FORM
    const statusAsCategoriesByForm = `
		count(distinct(case when form_status = 'complete' then asset_urn end)) as "Completed",
		count(distinct(case when form_status = 'in_progress' then asset_urn end)) as "In Progress",
		count(distinct(case when form_status = 'not_started' then asset_urn end)) as "Not Started"
	`;

    // Reusable select aggregate for status categories- only to be used BY ASSET
    const statusAsCategoriesByAsset = `
		${completeOrNotStartedCount('complete')} as "Completed",
		${inProgressCount} as "In Progress",
		${completeOrNotStartedCount('not_started')} as "Not Started"
	`;

    // Reusable select aggregate for prompt status categories
    const promptStatusAsCategories = `
		count(distinct(case when question_status = 'complete' then asset_urn end)) as "Completed",
		count(distinct(case when question_status = 'in_progress' then asset_urn end)) as "In Progress",
		count(distinct(case when question_status = 'not_started' then asset_urn end)) as "Not Started"
	`;

    // Reusable select for percentage completed
    const percentCompleted = `
		count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent
	`;

    // Reusable orderBy for top performing
    const orderByTopPerforming = `
		count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) desc
	`;

    // Reusable orderBy for least performing
    const orderByLeastPerforming = `
		count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) asc
	`;

    // Query skip logic
    const skip = () => {
        const base = !snapshotDate || !daysSinceDate;
        if (entity === 'form') return base || !formId;
        if (entity === 'assignee') return base || !assigneeId;
        if (entity === 'domain') return base || !domainId;
        return base;
    };

    return {
        /*
         * Chart Queries
         */

        // Completed Trend Stat Details
        // TODO: Validate use of `snapshot_date` vs `form_assigned_date`
        completedTrendPercentAndCount: queryBuilder(
            `select
				${completeOrNotStartedPercentAndCount('complete')}
			from
				'{{ table }}'
			where
				snapshot_date >= '${daysSinceDate}'
				and form_assigned_date >= '${daysSinceDate}';
			`,
        ),

        // Completed Trend Line Chart
        completedTrend: queryBuilder(
            `select
				${trend}
			from
				'{{ table }}'
			where
				form_assigned_date >= '${daysSinceDate}'
				and snapshot_date >= '${daysSinceDate}'
				and form_status = 'complete'
			group by
				asset_urn, form_assigned_date;
			`,
        ),

        // In Progress Trend Stat Details
        inProgressTrendPercentAndCount: queryBuilder(
            `select
				${inProgressPercentAndCount}
			from
				'{{ table }}'
			where
				snapshot_date >= '${daysSinceDate}'
				and form_assigned_date >= '${daysSinceDate}';
			`,
        ),

        // In Progress Trend Line Chart
        inProgressTrend: queryBuilder(
            `select
				${trend}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and form_status = 'in_progress'
			group by
				asset_urn, form_assigned_date;
			`,
        ),

        // Not Started Trend Stat Details
        notStartedTrendPercentAndCount: queryBuilder(
            `select
        		${completeOrNotStartedPercentAndCount('not_started')}
			from
				'{{ table }}'
			where
				snapshot_date >= '${daysSinceDate}'
				and form_assigned_date >= '${daysSinceDate}';
        	`,
        ),

        // Not Started Trend Line Chart
        notStartedTrend: queryBuilder(
            `select
				${trend}
			from
				'{{ table }}'
			where
				form_assigned_date >= '${daysSinceDate}'
				and snapshot_date >= '${daysSinceDate}'
				and form_status = 'not_started'
			group by
				asset_urn, form_assigned_date;
			`,
        ),

        // Status of Assets Bar Chart
        // TODO: Explore date (by month) aggregation for > 90 days
        docStatusByDate: queryBuilder(
            `select
				DATE_TRUNC('${dateTrunc()}', form_assigned_date) as 'date',
				${statusAsCategoriesByAsset}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				DATE_TRUNC('${dateTrunc()}', form_assigned_date)
			`,
        ),
        //			form_assigned_date;

        // Forms Bar Chart
        docProgressByForm: queryBuilder(
            `select
				form_urn as 'form',
				${statusAsCategoriesByForm}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_urn
			order by
				count(distinct(asset_urn)) asc;
			`,
        ),

        // Table view of form performance
        completionPerformanceByForm: queryBuilder(
            `select
				form_urn as 'form',
				${statusAsCategoriesByForm},
				count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_urn;
			`,
        ),

        // List of Top Performing Forms
        formTopPerforming: queryBuilder(
            `select
				form_urn as 'form',
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_urn
			order by
				${orderByTopPerforming}
			limit
				3;
			`,
        ),

        // List of Least Performing Forms
        formLeastPerforming: queryBuilder(
            `select
				form_urn as 'form',
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_urn
			order by
				${orderByLeastPerforming}
			limit
				3;
			`,
        ),

        // Question Bar Chart
        // Does not use the queryBuilder as it only appears on the `form` tab
        formQuestionProgress: `select
				question_id as 'question',
				${promptStatusAsCategories}
			from
				'{{ table }}'
			where
				form_urn = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				question_id
			order by
				count(distinct(case when form_status = 'complete' then asset_urn end)) desc;
			`,

        // Assignee Table View
        docProgressByAssignee: queryBuilder(
            `select
				assignee_urn,
				${statusAsCategoriesByAsset},
				${completeOrNotStartedCount('complete')} / count(distinct(asset_urn)) as completed_asset_percent,
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn;
			`,
        ),

        // List of Top Performing Assignees
        // TODO: Determine how useful/relevant/accurate is this to display?
        assigneeTopPerforming: queryBuilder(
            `select
				assignee_urn,
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn
			order by
				${orderByTopPerforming}
			limit
				5;
			`,
        ),

        // List of Least Performing Assignees
        // TODO: Determine how useful/relevant/accurate is this to display?
        assigneeLeastPerforming: queryBuilder(
            `select
				assignee_urn,
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn
			order by
				${orderByLeastPerforming}
			limit
				5;
			`,
        ),

        // Domain Bar Chart
        docProgressByDomain: queryBuilder(
            `select
				domain_urn,
				${statusAsCategoriesByAsset}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain_urn
			order by
				count(distinct(asset_urn)) asc;
			`,
        ),

        // Table view of form performance
        completionPerformanceByDomain: queryBuilder(
            `select
				domain_urn,
				${statusAsCategoriesByAsset},
				${completeOrNotStartedCount('complete')} / count(distinct(asset_urn)) as completed_asset_percent,
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain_urn;
			`,
        ),

        // List of Top Performing Domains
        domainTopPerforming: queryBuilder(
            `select
				domain_urn,
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain_urn
			order by
				${orderByTopPerforming}
			limit
				3;
			`,
        ),

        // List of Least Performing Domains
        domainLeastPerforming: queryBuilder(
            `select
				domain_urn,
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain_urn
			order by
				${orderByLeastPerforming}
			limit
				3;
			`,
        ),

        /*
         * Lists of Entities Queries
         */

        getFormsWithAnalytics: `select
				form_urn
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				form_urn,
			`,

        getAssignessWithFormAnalytics: `select
					assignee_urn
				from
					'{{ table }}'
				where
					snapshot_date = '${snapshotDate}'
					and form_assigned_date >= '${daysSinceDate}'
				group by
					assignee_urn;
				`,

        getDomainsWithFormAnalytics: `select
				domain_urn
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				domain_urn;
			`,

        /*
         * CSV Query
         */

        downloadCSVJSON: `select
				form_urn,
				form_assigned_date,
				form_completed_date,
				form_type,
				form_status,
				domain_urn,
				asset_urn,
				assignee_urn,
				count(distinct(case when question_status = 'complete' then question_id end)) as questions_complete,
				count(distinct(case when question_status = 'not_started' then question_id end)) as questions_not_started
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				${entity === 'form' && formId ? `and form_urn = '${formId}'` : ''}
				${entity === 'assignee' && assigneeId ? `and assignee_urn = '${assigneeId}'` : ''}
				${entity === 'domain' && domainId ? `and domain_urn = '${domainId}'` : ''}
			group by
				form_urn,
				form_assigned_date,
				form_completed_date,
				form_type,
				form_status,
				domain_urn,
				asset_urn,
				assignee_urn;
			`,

        /*
         * Query Utils
         */
        skip: skip(),
    };
};
