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
		const whereIndex = updatedQuery.indexOf('where') + 5;

		const injectWhere = (whereItem: string) =>
			`${updatedQuery.slice(0, whereIndex)} ${whereItem} and ${query.slice(whereIndex)}`;

		if (entity === 'form') updatedQuery = injectWhere(`form_id = '${formId}'`);
		if (entity === 'assignee') updatedQuery = injectWhere(`assignee_urn = '${assigneeId}'`);
		if (entity === 'domain') updatedQuery = injectWhere(`domain = '${domainId}'`);

		return updatedQuery;
	};

	// Date truncation
	const dateTrunc = () => {
		let trunc = 'day'; // last 7 days
		if (series === 30) trunc = 'day'; // last 30 days
		if (series === 90) trunc = 'week'; // last 90 days
		if (series === 365) trunc = 'year'; // last 365 days
		return trunc;
	}

	// Reusable select for percentage and count stats
	const percentAndCount = (status: string) => (`
		count(distinct(case when form_status = '${status}' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
		count(distinct(case when form_status = '${status}' then asset_urn end)) as completed_asset_count,
		count(distinct(asset_urn)) as assigned_asset_count
	`);

	// Reusable select for trend stats
	const trend = `
		snapshot_date as date,
		count(distinct(asset_urn)) as value
	`;

	// Reusable select aggregate for status categories
	const statusAsCategories = `
		count(distinct(case when form_status = 'complete' then asset_urn end)) as "Completed",
		count(distinct(case when form_status = 'in_progress' then asset_urn end)) as "In Progress",
		count(distinct(case when form_status = 'not_started' then asset_urn end)) as "Not Started"
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
	}

	return ({
		/* 
		* Chart Queries
		*/

		// Completed Trend Stat Details
		// TODO: Validate use of `snapshot_date` vs `form_assigned_date`
		completedTrendPercentAndCount: queryBuilder(
			`select
				${percentAndCount('complete')}
			from
				'{{ table }}'
			where
				snapshot_date >= '${daysSinceDate}'
				and form_assigned_date >= '${daysSinceDate}';
			`),

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
				snapshot_date;
			`),

		// In Progress Trend Stat Details
		inProgressTrendPercentAndCount: queryBuilder(
			`select
				${percentAndCount('in_progress')}
			from
				'{{ table }}'
			where
				snapshot_date >= '${daysSinceDate}'
				and form_assigned_date >= '${daysSinceDate}';
			`),

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
				form_assigned_date;
			`),

		// Not Started Trend Stat Details
		notStartedTrendPercentAndCount: queryBuilder(
			`select
				${percentAndCount('not_started')}
			from
				'{{ table }}'
			where
				snapshot_date >= '${daysSinceDate}'
				and form_assigned_date >= '${daysSinceDate}';
			`),

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
				form_assigned_date;
			`),

		// Status of Assets Bar Chart
		// TODO: Explore date (by month) aggregation for > 90 days
		docStatusByDate: queryBuilder(
			`select
				DATE_TRUNC('${dateTrunc()}', form_assigned_date) as 'date',
				${statusAsCategories}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				DATE_TRUNC('${dateTrunc()}', form_assigned_date),
				form_assigned_date;
			`),

		// Forms Bar Chart
		docProgressByForm: queryBuilder(
			`select
				form_id as 'form',
				${statusAsCategories}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id
			order by
				count(distinct(asset_urn)) asc;
			`),

		// Table view of form performance
		completionPerformanceByForm: queryBuilder(
			`select
				form_id as 'form',
				${statusAsCategories},
				count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id;
			`),

		// List of Top Performing Forms
		formTopPerforming: queryBuilder(
			`select
				form_id as 'form',
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id
			order by
				${orderByTopPerforming}
			limit
				3;
			`),

		// List of Least Performing Forms
		formLeastPerforming: queryBuilder(
			`select
				form_id as 'form',
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id
			order by
				${orderByLeastPerforming}
			limit
				3;
			`),

		// Question Bar Chart
		// Does not use the queryBuilder as it only appears on the `form` tab
		formQuestionProgress:
			`select
				question_id as 'question',
				${statusAsCategories}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
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
				${statusAsCategories},
				count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn;
			`),

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
			`),

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
			`),

		// Domain Bar Chart
		docProgressByDomain: queryBuilder(
			`select
				domain,
				${statusAsCategories}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain
			order by
				count(distinct(asset_urn)) asc;
			`),

		// Table view of form performance
		completionPerformanceByDomain: queryBuilder(
			`select
				domain,
				${statusAsCategories},
				count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain;
			`),

		// List of Top Performing Domains
		domainTopPerforming: queryBuilder(
			`select
				domain,
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain
			order by
				${orderByTopPerforming}
			limit
				3;
			`),

		// List of Least Performing Domains
		domainLeastPerforming: queryBuilder(
			`select
				domain,
				${percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain
			order by
				${orderByLeastPerforming}
			limit
				3;
			`),

		/* 
		* Lists of Entities Queries
		*/

		getFormsWithAnalytics:
			`select
				form_id
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				form_id,
			`,

		getAssignessWithFormAnalytics:
			`select
					assignee_urn
				from
					'{{ table }}'
				where
					snapshot_date = '${snapshotDate}'
					and form_assigned_date >= '${daysSinceDate}'
				group by
					assignee_urn;
				`,

		getDomainsWithFormAnalytics:
			`select
				domain
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				domain;
			`,

		/*
		* CSV Query
		*/

		downloadCSVJSON:
			`select
				form_assigned_date,
				count(form_status)
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				form_status,
				form_assigned_date;
			`,

		/* 
		* Query Utils 
		*/
		skip: skip(),
	});
}