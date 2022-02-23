- Start Date: (fill me in with today's date, 2022-02-22)
- RFC PR: https://github.com/linkedin/datahub/pull/4237 
- Discussion Issue: (GitHub issue this was discussed in before the RFC, if any)
- Implementation PR(s): (leave this empty)

# Extend data model to model DataDoc entity 

## Background
[Querybook](https://www.querybook.org/) is Pinterest’s open-source big data IDE via a notebook interface. 
We(Included Health) leverage it as our main querying tool. It has a feature, DataDoc, which organizes rich text, 
queries, and charts into a notebook to easily document analyses. People could work collaboratively with others in a 
DataDoc and get real-time updates. We believe it would be valuable to ingest the DataDoc metadata to Datahub and make
it easily searchable and discoverable by others.

## Summary
This RFC proposes the data model used to model DataDoc entity. It does not talk about any architecture, API or other 
implementation details. This RFC only includes minimum data model which could meet our initial goal. If the community 
decides to adopt this new entity, further effort is needed.

## Detailed design

### DataDoc 
![DataDoc High Level Model](datadoc-high-level-model.png) 

As shown in the above diagram, DataDoc is a document which contains a list of DataDoc cells. It organizes rich text,queries, and charts into a notebook 
to easily document analyses. It contains the following metadata:
- dataDocKey (keyAspect)
    - dataDocTool: The name of the DataDoc tool such as QueryBook, Notebook, and etc
    - dataDocId: Unique id for the DataDoc
- dataDocInfo
    - title(Searchable): The title of this DataDoc
    - description(Searchable): Detailed description about the DataDoc
    - lastModified: Captures information about who created/last modified/deleted this DataDoc and when
    - dataDocUrl: URL for the DataDoc.
- dataDocContent
    - content: The content of a DataDoc which is composed by a list of DataDocCell
- editableDataDocProperties
- ownership
- status
- globalTags
- institutionalMemory
- domains 

### DataDoc Cells
DataDoc cell is the unit that compose a DataDoc. There are three types of cells: Text Cell, Query Cell, Chart Cell. Each 
type of cell has its own metadata. Since the cell only lives within a DataDoc, we model cells as one aspect of DataDoc
rather than another entity. Here are the metadata of each type of cell:
- TextCell
    - cellTitle: Title of the cell
    - cellId: Unique id for the cell.
    - lastModified: Captures information about who created/last modified/deleted this DataDoc cell and when
    - text(searchable): The actual text in a TextCell in a DataDoc
- QueryCell
    - cellTitle: Title of the cell
    - cellId: Unique id for the cell.
    - lastModified: Captures information about who created/last modified/deleted this DataDoc cell and when
    - rawQuery(Searchable): Raw query to explain some specific logic in a DataDoc
    - lastExecuted: Captures information about who last executed this query cell and when
- ChartCell
    - cellTitle: Title of the cell
    - cellId: Unique id for the cell.
    - lastModified: Captures information about who created/last modified/deleted this DataDoc cell and when

## Future Work
Querybook provides an embeddable feature. We could embed a query tab which utilize the embedded feature in Datahub 
which provide a search-and-explore experience to user.
