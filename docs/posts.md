import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Posts

<FeatureAvailability/>
DataHub allows users to make Posts that can be displayed on the app. Currently, Posts are only supported on the Home Page, but may be extended to other surfaces of the app in the future. Posts can be used to accomplish the following:

* Allowing Admins to post announcements on the home page
* Pinning important DataHub assets or pages
* Pinning important external links

##  Posts Setup, Prerequisites, and Permissions

Anyone can view Posts on the home page. To create Posts, a user must either have the **Create Global Announcements** Privilege, or possess the **Admin** DataHub Role.

## Creating Posts

### Create Posts Using the UI
To create a post, first navigate to the Settings tab in the top-right menu of DataHub.
Once you're on the Settings page, click 'Home Page Posts'.
To create a new Post, click '+ New Post'.
<p align="center">
 <img alt="Creating a new post" width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/posts/new-post.png" />
</p>
DataHub currently supports two types of Post content. Posts can either be of type **Text** or **Link**. Click on "Post Type" to switch between these types.
<p align="center">
 <img alt="Selecting text post type" width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/posts/post-type-text.png" />
</p>
<p align="center">
 <img alt="Selecting link post type" width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/posts/post-type-link.png" />
</p>
If you choose the text type, enter the title and description as prompted; if you choose the link type, enter the title and the URL of the link and the address of the image as prompted.

Click 'Create' to complete.
<p align="center">
 <img alt="Viewing posts" width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/posts/view-posts.png" />
</p>

### Create Posts Using the GraphQL

To create a post via API, you can call the [createPost](../graphql/mutations.md#createPost) GraphQL mutation.
To create a post via API, you can call the [createPost](../graphql/mutations.md#createPost) GraphQL mutation.

There is only one type of Post that can be currently made, and that is a **Home Page Announcement**. This may be extended in the future to other surfaces.

DataHub currently supports two types of Post content. Posts can either contain **TEXT** or can be a **LINK**. When creating a post through GraphQL, users will have to supply the post content. 

For **TEXT** posts, the following pieces of information are required in the `content` object (of type [UpdatePostContentInput](../graphql/inputObjects.md#updatepostcontentinput)) of the GraphQL `input` (of type [CreatePostInput](../graphql/inputObjects.md#createpostinput))). **TEXT** posts cannot be clicked.
* `contentType: TEXT`
* `title`
* `description`

The `link` and `media` attributes are currently unused for **TEXT** posts.

For **LINK** posts, the following pieces of information are required in the `content` object (of type [UpdatePostContentInput](../graphql/inputObjects.md#updatepostcontentinput)) of the GraphQL `input` (of type [CreatePostInput](../graphql/inputObjects.md#createpostinput))). **LINK** posts redirect to the provided link when clicked.
* `contentType: LINK`
* `title`
* `link`
* `media`. Currently only the **IMAGE** type is supported, and the URL of the image must be provided

The `description` attribute is currently unused for **LINK** posts.

Here are some examples of Posts displayed on the home page, with one **TEXT** post and two **LINK** posts.

<p align="center">
 <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/posts/view-posts.png" />
</p>

### GraphQL

* [createPost](../graphql/mutations.md#createpost)
* [listPosts](../graphql/queries.md#listposts)
* [deletePosts](../graphql/queries.md#listposts)

### Examples

##### Create Post
```graphql
mutation test {
  createPost(
    input: {
      postType: HOME_PAGE_ANNOUNCEMENT, 
      content: {
        contentType: TEXT, 
        title: "Planed Upgrade 2023-03-23 20:05 - 2023-03-23 23:05", 
        description: "datahub upgrade to v0.10.1"
      }
    }
  )
}

```

##### List Post

```graphql
query listPosts($input: ListPostsInput!) {
  listPosts(input: $input) {
    start
    count
    total
    posts {
      urn
      type
      postType
      content {
        contentType
        title
        description
        link
        media {
          type
          location
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}

```
##### Input for list post
```shell
{
  "input": {
    "start": 0,
    "count": 10
  }
}
```

##### Delete Post

```graphql
mutation deletePosting { 
  deletePost (
    urn: "urn:li:post:61dd86fa-9e76-4924-ad45-3a533671835e"
  )
}
```
