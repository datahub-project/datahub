// Resuable Aggregates
const aggregate = {
	countFormStatus: {
		statusAsCategories: `
			count(distinct(case when form_status = 'complete' then asset_urn end)) as "Completed",
			count(distinct(case when form_status = 'in_progress' then asset_urn end)) as "In Progress",
			count(distinct(case when form_status = 'not_started' then asset_urn end)) as "Not Started"
		`,
	},
	percentages: {
		percentCompleted:
			`count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent`,
		orderByTopPerforming:
			`count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) desc`,
		orderByLeastPerforming:
			`count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) asc`
	},
}

// Query strings for the charts in the Governance Dashboard
export const sqlQueries = (
	daysSinceDate: string,
	formId?: string,
	assigneeId?: string,
	domainId?: string,
	snapshotDate?: string
) => ({
	/* 
	* Overall Tab
	*/
	overallAssignedStatus:
		`select
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null;
			`,

	overallDocProgressByDate:
		`select
				form_assigned_date as 'date',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_assigned_date;
			`,

	overallDocProgressByForm:
		`select
				form_id as 'form',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id;
			`,

	overallDocProgressByFormTopPerforming:
		`select
				form_id as 'form',
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id
			order by
				${aggregate.percentages.orderByTopPerforming}
			limit
				3;
			`,

	overallDocProgressByFormLeastPerforming:
		`select
				form_id as 'form',
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id
			order by
				${aggregate.percentages.orderByLeastPerforming}
			limit
				3;
			`,

	overallDocProgressByAssignee:
		`select
				assignee_urn,
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn;
			`,

	overallDocProgressByAssigneeTopPerforming:
		`select
				assignee_urn,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn
			order by
				${aggregate.percentages.orderByTopPerforming}
			limit
				5;
			`,

	overallDocProgressByAssigneeLeastPerforming:
		`select
				assignee_urn,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn
			order by
				${aggregate.percentages.orderByLeastPerforming}
			limit
				5;
			`,

	overallDocProgressByDomain:
		`select
				domain,
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain;
			`,

	overallDocProgressByDomainTopPerforming:
		`select
			domain,
			${aggregate.percentages.percentCompleted}
		from
			'{{ table }}'
		where
			snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}'
			and assignee_urn is not null
		group by
			domain
		order by
			${aggregate.percentages.orderByTopPerforming}
		limit
			3;
		`,

	overallDocProgressByDomainLeastPerforming:
		`select
				domain,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain
			order by
				${aggregate.percentages.orderByLeastPerforming}
			limit
				3;
			`,

	/* 
	* By Form Tab
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

	byFormOverallProgress:
		`select
				form_id as 'form',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id;
			`,

	byFormOverallProgressByDate:
		`select
				form_assigned_date as 'date',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_assigned_date;
			`,

	byFormByQuestionProgress:
		`select
				question_id as 'question',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				question_id;
			`,

	byFormOverallDocProgressByAssignee:
		`select
				assignee_urn,
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn;
			`,

	byFormDocProgressByAssigneeTopPerforming:
		`select
				assignee_urn,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn
			order by
				${aggregate.percentages.orderByTopPerforming}
			limit
				3;
			`,

	byFormDocProgressByAssigneeLeastPerforming:
		`select
				assignee_urn,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				assignee_urn
			order by
				${aggregate.percentages.orderByLeastPerforming}
			limit
				3;
			`,

	byFormOverallDocProgressByDomain:
		`select
				domain,
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain;
			`,

	byFormDocProgressByDomainTopPerforming:
		`select
			domain,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain
			order by
				${aggregate.percentages.orderByTopPerforming}
			limit
				3;
			`,

	byFormDocProgressByDomainLeastPerforming:
		`select
				domain,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				form_id = '${formId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain
			order by
				${aggregate.percentages.orderByLeastPerforming}
			limit
				3;
			`,

	/* 
	* By Assignee Tab
	*/
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

	byAssigneeOverallProgress:
		`select
				form_id as 'form',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				assignee_urn = '${assigneeId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id;
			`,

	byAssigneeOverallProgressByDate:
		`select
				form_assigned_date as 'date',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				assignee_urn = '${assigneeId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_assigned_date;
			`,

	byAssigneeOverallDocProgressByForm:
		`select
				form_id as 'form',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				assignee_urn = '${assigneeId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id;
			`,

	byAssigneeOverallDocProgressByFormTopPerforming:
		`select
				form_id as 'form',
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				assignee_urn = '${assigneeId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				form_id
			order by
				${aggregate.percentages.orderByTopPerforming}
			limit
				3;
			`,

	byAssigneeOverallDocProgressByFormLeastPerforming:
		`select
			form_id as 'form',
			${aggregate.percentages.percentCompleted}
		from
			'{{ table }}'
		where
			assignee_urn = '${assigneeId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}'
			and assignee_urn is not null
		group by
			form_id
		order by
			${aggregate.percentages.orderByLeastPerforming}
		limit
			3;
		`,

	byAssigneeOverallDocProgressByDomain:
		`select
				domain,
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				assignee_urn = '${assigneeId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain;
			`,

	byAssigneeDocProgressByDomainTopPerforming:
		`select
				domain,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				assignee_urn = '${assigneeId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain
			order by
				${aggregate.percentages.orderByTopPerforming}
			limit
				3;
			`,

	byAssigneeDocProgressByDomainLeastPerforming:
		`select
				domain,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				assignee_urn = '${assigneeId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
				and assignee_urn is not null
			group by
				domain
			order by
				${aggregate.percentages.orderByLeastPerforming}
			limit
				3;
			`,

	/* 
	* By Domain Tab
	*/
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

	byDomainOverallProgress:
		`select
				form_id as 'form',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				domain = '${domainId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				form_id;
			`,

	byDomainOverallProgressByDate:
		`select
				form_assigned_date as 'date',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				domain = '${domainId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				form_assigned_date;
			`,

	byDomainOverallDocProgressByForm:
		`select
				form_id as 'form',
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				domain = '${domainId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				form_id;
			`,

	byDomainOverallDocProgressByFormTopPerforming:
		`select
				form_id as 'form',
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				domain = '${domainId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				form_id
			order by
				${aggregate.percentages.orderByTopPerforming}
			limit
				3;
			`,

	byDomainOverallDocProgressByFormLeastPerforming:
		`select
				form_id as 'form',
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				domain = '${domainId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				form_id
			order by
				${aggregate.percentages.orderByLeastPerforming}
			limit
				3;
			`,

	byDomainOverallDocProgressByAssignee:
		`select
				assignee_urn,
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				domain = '${domainId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				assignee_urn;
			`,

	byDomainDocProgressByAssigneeTopPerforming:
		`select
				assignee_urn,
				${aggregate.percentages.percentCompleted}
			from
				'{{ table }}'
			where
				domain = '${domainId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				assignee_urn
			order by
				${aggregate.percentages.orderByTopPerforming}
			limit
				3;
			`,

	byDomainDocProgressByAssigneeLeastPerforming:
		`select
			assignee_urn,
			${aggregate.percentages.percentCompleted}
		from
			'{{ table }}'
		where
			domain = '${domainId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}'
		group by
			assignee_urn
		order by
			${aggregate.percentages.orderByLeastPerforming}
		limit
			3;
		`,

	byDomainOverallDocProgressByDomain:
		`select
				domain,
				${aggregate.countFormStatus.statusAsCategories}
			from
				'{{ table }}'
			where
				domain = '${domainId}'
				and snapshot_date = '${snapshotDate}'
				and form_assigned_date >= '${daysSinceDate}'
			group by
				domain;
			`,

	/* 
	* Stats Analytics by Form
	*/
	completedTrendPercentAndCount:
		`select
			count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			count(distinct(asset_urn)) as assigned_asset_count
		from
			'{{ table }}'
		where
			form_id = '${formId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}';
		`,

	completedTrend:
		`select
			form_completed_date as 'date',
			count(distinct(asset_urn)) as 'value'
		from
			'{{ table }}'
		where
			form_id = '${formId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}'
			and form_status = 'complete'
		group by
			form_completed_date;
		`,

	notStartedTrendPercentAndCount:
		`select
			count(distinct(case when form_status = 'not_started' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			count(distinct(asset_urn)) as assigned_asset_count
		from
			'{{ table }}'
		where
			form_id = '${formId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}';
		`,

	notStartedTrend:
		`select
			form_completed_date as 'date',
			count(distinct(asset_urn)) as 'value'
		from
			'{{ table }}'
		where
			form_id = '${formId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}'
			and form_status = 'not_started'
		group by
			form_completed_date;
		`,

	/* 
	* Stats Analytics by Asignee
	*/
	completedTrendPercentAndCountByAssignee:
		`select
			count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			count(distinct(asset_urn)) as assigned_asset_count
		from
			'{{ table }}'
		where
			assignee_urn = '${assigneeId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}';
		`,

	completedTrendByAssignee:
		`select
			form_completed_date as 'date',
			count(distinct(asset_urn)) as 'value'
		from
			'{{ table }}'
		where
			assignee_urn = '${assigneeId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}'
			and form_status = 'complete';
		group by
			form_completed_date;
		`,

	notStartedTrendPercentAndCountByAssignee:
		`select
			count(distinct(case when form_status = 'not_started' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			count(distinct(asset_urn)) as assigned_asset_count
		from
			'{{ table }}'
		where
			assignee_urn = '${assigneeId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}';
		`,

	notStartedTrendByAssignee:
		`select
			form_completed_date as 'date',
			count(distinct(asset_urn)) as 'value'
		from
			'{{ table }}'
		where
			assignee_urn = '${assigneeId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}'
			and form_status = 'not_started';
		group by
			form_completed_date;
		`,

	/* 
	* Stats Analytics by Domain
	*/
	completedTrendPercentAndCountByDomain:
		`select
			count(distinct(case when form_status = 'complete' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			count(distinct(asset_urn)) as assigned_asset_count
		from
			'{{ table }}'
		where
			domain = '${domainId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}';
		`,

	completedTrendByDomain:
		`select
			form_completed_date as 'date',
			count(distinct(asset_urn)) as 'value'
		from
			'{{ table }}'
		where
			domain = '${domainId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}'
			and form_status = 'complete';
		group by
			form_completed_date;
		`,

	notStartedTrendPercentAndCountByDomain:
		`select
			count(distinct(case when form_status = 'not_started' then asset_urn end)) / count(distinct(asset_urn)) as completed_asset_percent,
			count(distinct(asset_urn)) as assigned_asset_count
		from
			'{{ table }}'
		where
			domain = '${domainId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}';
		`,

	notStartedTrendByDomain:
		`select
			form_completed_date as 'date',
			count(distinct(asset_urn)) as 'value'
		from
			'{{ table }}'
		where
			domain = '${domainId}'
			and snapshot_date = '${snapshotDate}'
			and form_assigned_date >= '${daysSinceDate}'
			and form_status = 'not_started'
		group by
			form_completed_date;
		`,

	/*
	*	CSV Export
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
});