/**
 * Defines the available BaseEntity values
 * @export
 * @enum {string}
 */
export enum BaseEntity {
  Company = 'Company',
  FieldOfStudy = 'FieldofStudy',
  Job = 'Job',
  Member = 'Member',
  School = 'School',
  Skill = 'Skill',
  Title = 'Title'
}

/**
 * Defines the available Classification values
 * @export
 * @enum {string}
 */
export enum Classification {
  Activity = 'Activity',
  Characteristics = 'Characteristics'
}

/**
 * Maps each classification value to supported Category hierarchy values
 */
export const Category: Record<Classification, Array<string>> = {
  [Classification.Activity]: [
    'Ads.Click',
    'Ads.Click.Compaign Type',
    'Ads.Conversion',
    'Ads.Conversion.Action Type',
    'Ads.Conversion.Compaign Type',
    'Ads.View',
    'Ads.View.Compaign Type',
    'Feed',
    'Feed.Click, Save, Comment, Like.Channel',
    'Feed.Like',
    'Feed.Message',
    'Feed.Post',
    'Feed.Reshare.Message',
    'Feed.Reshare.Post',
    'Feed.Share',
    'Feed.Share.ApiShareFlag',
    'Feed.Share.Format',
    'Feed.Share.Post',
    'Feed.Share.Type',
    'Job',
    'Job.Apply, Save',
    'Job.Dismiss',
    'Job.Inmail',
    'Job.SearchQuery',
    'Job.View',
    'Member',
    'Member, Job',
    'Member, Job.Inmail, Post',
    'Member.Respond Inmail.Inmail Type',
    'Member.Respond Inmail.Sender Type',
    'Message.Receive.Inmail',
    'Message.Receive.Other',
    'Notification.Receive.Platform',
    'Page.View',
    'Page.View.Page Group',
    'Page.View.Platform',
    'Page.View.Portal',
    'Page.View.UI Type',
    'Search',
    'Search.Search',
    'Search.Search.Companies',
    'Search.Search.Content',
    'Search.Search.Edu',
    'Search.Search.Federated',
    'Search.Search.Groups',
    'Search.Search.Jobs',
    'Search.Search.People',
    'Search.View'
  ],
  [Classification.Characteristics]: [
    'General',
    'General.CompanyStandardizedData',
    'General.EducationDerivedData',
    'General.Embedding',
    'General.ExperienceLevelStandardizedData',
    'General.ExperienceLevelStdData',
    'General.GeoStandardizedData',
    'General.GeoStdData',
    'General.IndustryStandardizedData',
    'General.Network',
    'General.PositionDerivedData',
    'General.SkillsStandardizedData',
    'General.SkillsStdData',
    'General.TermVector',
    'General.TitleStandardizedData',
    'Job.CompanyStandardizedData',
    'Job.GeoStandardizedData',
    'Job.IndustryStandardizedData',
    'Job.TitleStandardizedData',
    'Member.EducationDerivedData',
    'Member.Targeting'
  ]
};
