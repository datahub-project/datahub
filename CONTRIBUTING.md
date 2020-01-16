# Contributing

We always welcome contributions to help make WhereHows better. Take a moment to read this document if you would like to contribute.

## Reporting issues

We use GitHub issues to track bug reports, feature requests, and submitting pull requests.

If you find a bug:

1. Use the GitHub issue search to check whether the bug has already been reported.

1. If the issue has been fixed, try to reproduce the issue using the latest master branch of the repository.

1. If the issue still reproduces or has not yet been reported, try to isolate the problem before opening an issue.

## Submitting a Pull Request (PR)

Before you submit your Pull Request (PR), consider the following guidelines:

* Search GitHub for an open or closed PR that relates to your submission. You don't want to duplicate effort.
* Make your changes in a new branch:

```  
  git checkout -b my-fix-branch master
```  

* Create your patch, *including appropriate test cases*.
* Run the test suite
* Commit your changes using a descriptive commit message that follows our [commit message format](#commit-message-format).

```
  git commit -a
```  

Note: The optional commit -a command-line option will automatically "add" and "rm" edited files.

* Push your branch to GitHub:

  git push origin my-fix-branch
  
* In GitHub, send a pull request to `Wherehows:master`
* If we suggest changes, then:
  * Make the required updates.
  * Re-run the test suite
  * Rebase your branch and force push to your GitHub repository (this will update your Pull Request)

```
  git rebase master -i
  git push -f
```
  
That's it! Thank you for your contribution!

## After your pull request is merged

After your pull request is merged, you can safely delete your branch and pull the changes from the main (upstream) repository:

* Delete the remote branch on GitHub either through the GitHub web UI or your local shell as follows:

```
  git push origin --delete my-fix-branch
```

* Check out the master branch:

```
  git checkout master -f
```

* Update your master with the latest upstream version:

```
  git pull --ff upstream master
```

## Commit Message Format

Each commit message consists of a *header*, a *body* and a *footer*. The header has a special formation that includes a *type*, a *scope*, and a *subject*:

    <type>: <subject>
    <BLANK LINE>
    <body>
    <BLANK LINE>
    <footer>

The *header* is mandatory.

Any line of the commit message cannot be longer than 88 characters! This allows the message to be easier to read on GitHub as well as in various Git tools.

### Revert

If the commit reverts a previous commit, it should begin with `revert:`, followed by the header of the reverted commit. In the body it should say: `This reverts commit <hash>`, where the hash is the SHA of the commit being reverted.

### Type

Must be one of the following:

* *feat*: A new feature
* *fix*: A bug fix
* *docs*: Documentation changes only
* *style*: Changes that do not affect the meaning of the code (whitespace, formatting, missing semicolons, etc.)
* *refactor*: A code change that neither fixes a bug nor adds a feature
* *test*: Adding missing tests
* *chore*: Changes to the build process or auxiliary tools and libraries, such as documentation generation

### Subject

The subject contains a succinct description of the change:

* use the imperative, present tense: "change" not "changed" nor "changes"
* don't capitalize the first letter
* no dot(.) at the end

### Body

Just as in the subject, use the imperative, present tense: "change" not "changed" nor "changes". The body should include the motivation for the change and contrast this with previous behavior.

### Footer

The footer should contain any information about *Breaking Changes*, and is also the place to reference GitHub issues that this commit *Closes*.

*Breaking Changes* should start with the words `BREAKING CHANGE:` with a space or two new lines. The rest of the commit message is then used for this.

