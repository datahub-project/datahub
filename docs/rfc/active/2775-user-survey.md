Start Date: 2021-6-17
RFC PR: https://github.com/linkedin/datahub/pull/2775
Discussion Issue: https://github.com/linkedin/datahub/issues/2685 
Implementation PR(s): 
Link to Google Doc: https://docs.google.com/document/d/19XZOM7hGV03ruwhzcFA0wN-58is6Lq5-Go9Kalt_HpU/edit#

**DataHub User Survey**
Contact: melinda.cardenas@nytimes.com

**Summary**
This RFC proposes to add an in UI user survey to gauge users experience with DataHub. The purpose of this survey is to know whether users of DataHub are able to find what they’re looking for, whether users ran into any problems, and to know their overall impression of the site. See list of possible questions [here](https://docs.google.com/document/d/1m1GyW-Wkc3nvf1eMa5bt3HFgk0sgMnstmR6gc-c6lgw/edit). The survey questions will be configurable so that different questions of the same type (i.e. yes/no, free form, rate on a scale) can be easily swapped out in the frontend. For example, in a static config file, a yes/no question can be swapped out with another yes/no question, and the same goes for other types of questions. 

**Basic Example**
See Figma design [here](https://www.figma.com/file/slcjP197EJUthWM5q7od3n/DataHub-Survey?node-id=0%3A1)

**Motivation**
Having a way for users to provide immediate feedback about DataHub will give insight as to whether the tool successfully solves the data discovery problems users face, and will provide useful feedback for guiding how DataHub can improve in the future. This survey would be especially useful as DataHub gets integrated into different environments, since the problems and user experiences at each company will likely be different. 

**Non-Requirements**
For a future iteration of this project, cookies can be incorporated to better customize when surveys appear (in addition to having the option to click on a tab to open the survey), and what questions are asked. This way, users can be asked questions specific to the part of DataHub they’re using. If users decide to never be asked for their feedback again, that can be saved as well. 

**Detailed Design**
**Backend** 
- Feedback data will be modeled in PDL and include the results of a question. This "SurveyResponse" object will be in the metadata-models directory under "com/linkedin/feedback"
- Survey responses will be saved in an SQL table. Since DataHub uses Ebean objects to model SQL tables, we will create one for this table as well.
- “CREATE TABLE” statements will be added to the init.sql file, which executes on initialization of a new SQL DB container
- Create and read endpoints will be added to a “feedback” REST Resource 
- A GraphQL Mutation will be implemented to write a “SurveyResponse” by using the create endpoint. 
- Survey results can be read by either querying the underlying SQL table directly or using the "read" API 
- Link to ERD [here](https://dbdiagram.io/d/60d610d5dd6a5971481ffae7) or view photo in [google doc](https://docs.google.com/document/d/19XZOM7hGV03ruwhzcFA0wN-58is6Lq5-Go9Kalt_HpU/edit#). Alternatives we considered are [this](https://dbdiagram.io/d/60d61654dd6a597148200178) (version 2, which includes a question table) and [this](https://dbdiagram.io/d/60d0dc070c1ff875fcd5d251) (version 3, which is much more complex). 
- We decided to only use a survey_responses table since the questions will be coming from a static config file and not a table. 


**Frontend**
- For a first iteration, there will only be a single survey with three questions which will appear when a button at the bottom of the viewport is clicked (as shown in Figma). Ideally, this button will appear at the bottom of every page. 
- A form component will capture survey results and execute a GraphQL mutation that will create a “SurveyResponse”. 
- Questions will be statically configured in a yaml or json config somewhere in the frontend directory, kind of like this. Expected responses will also be included in the config file so it can be clear what will go in the table. Ex: far right smiley (refer to Figma) would be “really good” because that's the value that would be stipulated in the config. 

**Unresolved Questions**
I’m curious to know people’s opinions on the ERD above.
- Do people think having a separate questions table makes sense just for record keeping/organization? On one hand, having a questions table allows for more complexity later on if people want to build on top. On the other hand, it might be redundant/unnecessary to have both a questions table and questions config file. 
