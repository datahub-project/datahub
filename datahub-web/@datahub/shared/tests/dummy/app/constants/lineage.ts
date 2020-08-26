type Graph = Com.Linkedin.Metadata.Graph.Graph;

export const rootNode: Graph = {
  edges: [
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_lts_campaigns,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.campaign_sf_info,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.campaign_sf_info,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.adimpressions_mi,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.adimpressions_mi,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.engagement_union,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.engagement_union,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_engagement,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.engagement_union,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.mmb_touchpoints_desc,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.engagement_union,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.product_engagement,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.campaign_sf_info,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.lts_ins_core_monthly_media_campaigns,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.campaign_sf_info,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_offsite_lts_imps,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_offsite_lts_imps,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_monthly_lts_imps,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.campaign_sf_info,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_onsite_lts_imps,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_onsite_lts_imps,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_monthly_lts_imps,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,prod_foundation_tables.dim_f_sas_campaign_v2,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_lts_campaigns,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,prod_foundation_tables.dim_f_sas_member_advertiser_v2,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_lts_campaigns,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.manual_lts_account_add,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_lts_campaigns,EI)'
    },
    {
      fromNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_metrics.dim_lms_gso_info,EI)',
      toNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_lts_campaigns,EI)'
    }
  ],
  rootNode: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_lts_campaigns,EI)',
  nodes: [
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_lts_campaigns,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_lts_campaigns,EI)',
      displayName: 'u_gsoins.all_lts_campaigns'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.campaign_sf_info,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.campaign_sf_info,EI)',
      displayName: 'u_gsoins.campaign_sf_info'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.adimpressions_mi,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.adimpressions_mi,EI)',
      displayName: 'u_gsoins.adimpressions_mi'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.engagement_union,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.engagement_union,EI)',
      displayName: 'u_gsoins.engagement_union'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_engagement,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.all_engagement,EI)',
      displayName: 'u_gsoins.all_engagement'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.mmb_touchpoints_desc,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.mmb_touchpoints_desc,EI)',
      displayName: 'u_gsoins.mmb_touchpoints_desc'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.product_engagement,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.product_engagement,EI)',
      displayName: 'u_gsoins.product_engagement'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.lts_ins_core_monthly_media_campaigns,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.lts_ins_core_monthly_media_campaigns,EI)',
      displayName: 'u_gsoins.lts_ins_core_monthly_media_campaigns'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_offsite_lts_imps,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_offsite_lts_imps,EI)',
      displayName: 'u_gsoins.temp_offsite_lts_imps'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_monthly_lts_imps,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_monthly_lts_imps,EI)',
      displayName: 'u_gsoins.temp_monthly_lts_imps'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_onsite_lts_imps,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.temp_onsite_lts_imps,EI)',
      displayName: 'u_gsoins.temp_onsite_lts_imps'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,prod_foundation_tables.dim_f_sas_campaign_v2,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,prod_foundation_tables.dim_f_sas_campaign_v2,EI)',
      displayName: 'prod_foundation_tables.dim_f_sas_campaign_v2'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,prod_foundation_tables.dim_f_sas_member_advertiser_v2,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,prod_foundation_tables.dim_f_sas_member_advertiser_v2,EI)',
      displayName: 'prod_foundation_tables.dim_f_sas_member_advertiser_v2'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.manual_lts_account_add,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_gsoins.manual_lts_account_add,EI)',
      displayName: 'u_gsoins.manual_lts_account_add'
    },
    {
      attributes: [
        { name: 'fabric', value: 'EI' },
        { name: 'platform', value: 'hive' },
        { name: 'owners', value: '[]' }
      ],
      id: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_metrics.dim_lms_gso_info,EI)',
      entityUrn: 'urn:li:dataset:(urn:li:dataPlatform:hive,u_metrics.dim_lms_gso_info,EI)',
      displayName: 'u_metrics.dim_lms_gso_info'
    }
  ]
};
