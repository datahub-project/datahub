class PowerBiDaxQuery:
    GET_NEW_USAGE_METRICS_REPORT_PAGE_VIEWS: str = """EVALUATE SELECTCOLUMNS (
        ADDCOLUMNS (
            SUMMARIZECOLUMNS (
                'Report page views'[ReportId],
                'Report page views'[SectionId],
                'Report page views'[Date],
                'Report page views'[UserKey],
                FILTER (
                    'Report page views',
                    'Report page views'[Date] > TODAY()-{usage_stats_interval}
                ),
                "views_count", COUNTROWS('Report page views')
            ),
            "SectionName", LOOKUPVALUE(
                'Report pages'[SectionName],
                'Report pages'[ReportId], 'Report page views'[ReportId],
                'Report pages'[SectionId], 'Report page views'[SectionId]
            ),
            "UserId", LOOKUPVALUE(
                'Users'[UserId],
                'Users'[UserKey], 'Report page views'[UserKey]
            )
        ),
        "entity_id", 'Report page views'[ReportId],
        "sub_entity_id", [SectionName],
        "date", 'Report page views'[Date],
        "user_id", [UserId],
        "views_count", [views_count]
    )"""

    GET_NEW_USAGE_METRICS_REPORT_VIEWS: str = """EVALUATE SELECTCOLUMNS (
        SUMMARIZECOLUMNS (
            'Report views'[ReportId],
            'Report views'[Date],
            'Report views'[UserId],
            FILTER (
                'Report views',
                'Report views'[Date] > TODAY()-{usage_stats_interval}
            ),
            "views_count", COUNTROWS('Report views')
        ),
        "entity_id", 'Report views'[ReportId],
        "date", 'Report views'[Date],
        "user_id", 'Report views'[UserId],
        "views_count", [views_count]
    )"""

    GET_OLD_USAGE_METRICS_REPORT_VIEWS: str = """EVALUATE SELECTCOLUMNS (
        SUMMARIZECOLUMNS (
            'Views'[ReportGuid],
            'Views'[ReportPage],
            'Views'[Date],
            'Views'[UserGuid],
            FILTER (
                'Views',
                'Views'[Date] > TODAY()-{usage_stats_interval}
            ),
            "views_count", SUM('Views'[GranularViewsCount])
        ),
        "entity_id", 'Views'[ReportGuid],
        "sub_entity_id", 'Views'[ReportPage],
        "date", 'Views'[Date],
        "user_id", 'Views'[UserGuid],
        "views_count", [views_count]
    )"""

    GET_OLD_USAGE_METRICS_DASHBOARD_VIEWS: str = """EVALUATE SELECTCOLUMNS (
        SUMMARIZECOLUMNS (
            'Views'[DashboardGuid],
            'Views'[Date],
            'Views'[UserGuid],
            FILTER (
                'Views',
                'Views'[Date] > TODAY()-{usage_stats_interval}
            ),
            "views_count", SUM('Views'[GranularViewsCount])
        ),
        "entity_id", 'Views'[DashboardGuid],
        "date", 'Views'[Date],
        "user_id", 'Views'[UserGuid],
        "views_count", [views_count]
    )"""
