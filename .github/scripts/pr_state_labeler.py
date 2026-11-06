#!/usr/bin/env python3
"""
PR State Labeler

Manages review-state labels on open PRs in datahub-project/datahub. Run on a
schedule by .github/workflows/pr-state-labeler.yml with the workflow's
GITHUB_TOKEN; tested by test-github-scripts.yml.

Labels managed (mutually exclusive, highest precedence first):
  merge-pending-ci           - Approved, waiting for CI to merge (external contributor)
  pending-submitter-merge    - Approved, internal contributor should merge
  pending-submitter-response - Reviewer has responded, waiting for submitter
  needs-review               - PR is waiting for reviewer attention

Only PRs updated in the last --updated-within-hours are checked. Every input to
the state machine (reviews, comments, commits, label changes) bumps a PR's
updated_at, so PRs outside the window would get the same decision as last run.

Usage:
    # Dry run (preview only)
    GITHUB_TOKEN=... python3 .github/scripts/pr_state_labeler.py --dry-run

    # Re-check every open PR, not just recently updated ones
    GITHUB_TOKEN=... python3 .github/scripts/pr_state_labeler.py --dry-run --updated-within-hours 0
"""

import argparse
import functools
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

import github
import github.PullRequest
import github.PullRequestReview
import github.Repository

# ============================================================================
# CONFIGURATION
# ============================================================================

NEEDS_REVIEW = "needs-review"
PENDING_SUBMITTER_RESPONSE = "pending-submitter-response"
PENDING_SUBMITTER_MERGE = "pending-submitter-merge"
MERGE_PENDING_CI = "merge-pending-ci"

# Ordered by precedence. If multiple labels are set, the highest precedence label is used.
STATE_LABELS = [
    MERGE_PENDING_CI,
    PENDING_SUBMITTER_MERGE,
    PENDING_SUBMITTER_RESPONSE,
    NEEDS_REVIEW,
]

TERMINAL_STATES = {MERGE_PENDING_CI, PENDING_SUBMITTER_MERGE}

# Overlap generously with the schedule interval so a delayed or failed run
# doesn't leave PRs unchecked.
DEFAULT_UPDATED_WITHIN_HOURS = 3

logger = logging.getLogger(__name__)


# ============================================================================
# PR PROCESSOR
# ============================================================================

class GithubPRProcessor:
    def __init__(
        self,
        repo_name: str,
        dry_run: bool = True,
        limit: Optional[int] = None,
        updated_within_hours: int = DEFAULT_UPDATED_WITHIN_HOURS,
        ignored_label_actors: Optional[Set[str]] = None,
    ):
        self.dry_run = dry_run
        self.limit = limit
        self.repo_name = repo_name
        self.updated_within_hours = updated_within_hours
        # Label events from these logins are never treated as manual changes,
        # e.g. the account a previous version of this automation ran as.
        self.ignored_label_actors = ignored_label_actors or set()

        token = os.environ["GITHUB_TOKEN"]
        self.github_client = github.Github(auth=github.Auth.Token(token))
        self.repo = self.github_client.get_repo(repo_name)

    @staticmethod
    def _get_current_state_label(labels: Set[str]) -> Optional[str]:
        """Get the highest-precedence state label, if any."""
        for label in STATE_LABELS:
            if label in labels:
                return label
        return None

    def _set_current_state_label(
        self,
        pr: github.PullRequest.PullRequest,
        current_labels: Set[str],
        new_label: str,
    ) -> None:
        """Make new_label the PR's only state label.

        Removes every other state label (they're mutually exclusive), not just
        the highest-precedence one. Order matters if an API call fails midway:
        - non-terminal target: add first. A failed remove leaves two state
          labels, which the next run cleans up.
        - terminal target: remove first. Searches exclude terminal-labeled PRs,
          so a terminal label left alongside a stale one would never be cleaned
          up. A failed add instead leaves no state label, and the removal bumps
          updated_at, so the next run re-evaluates the PR.
        """
        mode = "[dry run] " if self.dry_run else ""
        stale = [
            label for label in STATE_LABELS if label in current_labels and label != new_label
        ]
        needs_add = new_label not in current_labels

        if not needs_add and not stale:
            logger.info(f"PR #{pr.number}: {new_label} is already the current state")
            return

        change = f"Adding {new_label}" if needs_add else f"Keeping {new_label}"
        if stale:
            change += f" and removing {', '.join(stale)}"
        logger.info(f"PR #{pr.number}: {mode}{change}")
        if self.dry_run:
            return
        if new_label in TERMINAL_STATES:
            for label in stale:
                pr.remove_from_labels(label)
            if needs_add:
                pr.add_to_labels(new_label)
        else:
            if needs_add:
                pr.add_to_labels(new_label)
            for label in stale:
                pr.remove_from_labels(label)

    def _has_manual_label_changes(
        self,
        pr: github.PullRequest.PullRequest,
    ) -> Optional[datetime]:
        """Return timestamp of most recent collaborator-driven label change.

        Excludes bots (including this workflow's own github-actions[bot]
        changes) and any login in ignored_label_actors.
        """
        collaborators = self._get_collaborators()

        latest_ts: Optional[datetime] = None
        for event in pr.get_issue_events():
            if (
                event.event in ("labeled", "unlabeled")
                and event.actor is not None
                and event.actor.type != "Bot"
                and event.actor.login not in self.ignored_label_actors
                and event.actor.login in collaborators
            ):
                if latest_ts is None or event.created_at > latest_ts:
                    latest_ts = event.created_at

        return latest_ts

    @staticmethod
    def _get_latest_reviews(
        pr: github.PullRequest.PullRequest,
    ) -> Dict[str, github.PullRequestReview.PullRequestReview]:
        """Return a dict of reviewer login → their latest review."""
        latest_reviews: Dict[str, github.PullRequestReview.PullRequestReview] = {}
        for review in pr.get_reviews():
            latest_reviews[review.user.login] = review
        return latest_reviews

    @staticmethod
    def _get_latest_approver_activity(
        approvers: Set[str],
        latest_reviews: Dict[str, github.PullRequestReview.PullRequestReview],
        comments: List,
        since: Optional[datetime] = None,
    ) -> Optional[datetime]:
        latest_review_time = max(
            (
                review.submitted_at
                for user, review in latest_reviews.items()
                if user in approvers and not (since and review.submitted_at < since)
            ),
            default=None,
        )

        latest_approver_comment_time = max(
            (
                comment.created_at
                for comment in comments
                if comment.user.login in approvers
                and not (since and comment.created_at < since)
            ),
            default=None,
        )

        return max(
            filter(None, [latest_review_time, latest_approver_comment_time]),
            default=None,
        )

    @staticmethod
    def _get_submitter_activity_since_review(
        pr: github.PullRequest.PullRequest,
        approvers: Set[str],
        comments: List,
        since: datetime,
    ) -> List[str]:
        """Return list of activity types (comments/commits) by the submitter since the last review."""
        activity = []

        has_recent_comments = any(
            comment.created_at > since
            for comment in comments
            if comment.user.login not in approvers and comment.user.type != "Bot"
        )
        if has_recent_comments:
            activity.append("comments")

        # Committer date, not author date: an amended or rebased commit keeps its
        # original author date but is (re)committed when it's pushed.
        has_recent_commits = any(
            commit.commit.committer.date > since and len(commit.parents) <= 1
            for commit in pr.get_commits()
        )
        if has_recent_commits:
            activity.append("commits")

        return activity

    @functools.cache
    def _get_collaborators(self) -> Set[str]:
        try:
            collaborators = list(self.repo.get_collaborators())
        except github.GithubException as e:
            if e.status == 403:
                raise RuntimeError(
                    "Token cannot list repository collaborators (403). The "
                    "collaborator list decides who counts as a reviewer and which "
                    "PRs are internal; check the workflow's token permissions."
                ) from e
            raise
        return {collab.login for collab in collaborators}

    def process_single_pr(self, pr: github.PullRequest.PullRequest) -> None:
        """Process a single PR according to the state machine rules."""
        current_labels = {label.name for label in pr.labels}
        pr_author = pr.user.login

        collaborators = self._get_collaborators()
        approvers = collaborators.difference({pr_author})
        is_internal = (
            pr_author in collaborators
            and "community-contribution" not in current_labels
        )

        if not current_labels:
            logger.info(f"PR #{pr.number}: Skipping as it's too new (no labels)")
            return

        current_state = self._get_current_state_label(current_labels)

        # Terminal states — skip all expensive API calls.
        if current_state in TERMINAL_STATES:
            logger.info(f"PR #{pr.number}: {current_state} is set (terminal state)")
            return

        most_recent_manual_label_change = self._has_manual_label_changes(pr)

        latest_reviews = self._get_latest_reviews(pr)

        # Fetch comments once and reuse across helper methods.
        comments = list(pr.get_issue_comments())

        most_recent_approver_activity = self._get_latest_approver_activity(
            approvers, latest_reviews, comments, since=most_recent_manual_label_change
        )

        _has_review_approval = any(
            review.state == "APPROVED"
            for user, review in latest_reviews.items()
            if user in approvers
        )
        _has_changes_requested = any(
            review.state == "CHANGES_REQUESTED" for review in latest_reviews.values()
        )
        has_approval = _has_review_approval and not _has_changes_requested

        set_state = functools.partial(self._set_current_state_label, pr, current_labels)

        # State machine
        if has_approval:
            if is_internal:
                set_state(PENDING_SUBMITTER_MERGE)
            else:
                set_state(MERGE_PENDING_CI)
        elif not most_recent_approver_activity and not most_recent_manual_label_change:
            # No reviews yet.
            set_state(NEEDS_REVIEW)
        elif (
            # By this point, at least one of the two timestamps must be set.
            (since_ts := most_recent_approver_activity or most_recent_manual_label_change)
            and (submitter_activity := self._get_submitter_activity_since_review(
                pr,
                approvers=approvers,
                comments=comments,
                since=since_ts,
            ))
        ):
            logger.info(
                f"PR #{pr.number}: New submitter {' and '.join(submitter_activity)} "
                f"detected since last review"
            )
            set_state(NEEDS_REVIEW)
        else:
            if (
                most_recent_manual_label_change
                and not most_recent_approver_activity
                and current_state is not None
            ):
                # Manual label change is most recent — respect it.
                logger.info(
                    f"PR #{pr.number}: Skipping — manual label changes with no other activity since"
                )
            elif most_recent_manual_label_change:
                # Manual change but no state label — treat as approver looking at PR.
                logger.info(
                    f"PR #{pr.number}: After manual label change, no state label; "
                    f"adding {PENDING_SUBMITTER_RESPONSE}"
                )
                set_state(PENDING_SUBMITTER_RESPONSE)
            else:
                # Approver reviewed, submitter has not responded.
                set_state(PENDING_SUBMITTER_RESPONSE)

    def process_prs(self) -> None:
        """Process open PRs and update their labels per the state machine rules."""
        # Exclude terminal-state PRs and pending-internal-resolution from the search.
        query = (
            f"repo:{self.repo_name} is:pr is:open -is:draft "
            f"sort:updated-desc "
            f"-label:pending-internal-resolution "
            f"-label:{MERGE_PENDING_CI} "
            f"-label:{PENDING_SUBMITTER_MERGE}"
        )
        if self.updated_within_hours > 0:
            since = datetime.now(timezone.utc) - timedelta(hours=self.updated_within_hours)
            query += f" updated:>={since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        logger.info(f"Search: {query}")

        # Load collaborators once up front: a permissions error here must fail
        # the run, not be caught and skipped by the per-PR handler below.
        self._get_collaborators()

        prs = self.github_client.search_issues(query)

        processed = 0
        for issue in prs:
            if self.limit is not None and processed >= self.limit:
                logger.info(f"Reached limit of {self.limit} PRs, stopping.")
                break

            try:
                pr = self.repo.get_pull(issue.number)
            except github.GithubException as e:
                if e.status == 404:
                    logger.warning(
                        f"PR #{issue.number}: Not found (404) — skipping. "
                        f"It may have been deleted or transferred."
                    )
                    continue
                raise

            logger.info(
                f"Processing PR https://github.com/{self.repo_name}/pull/{pr.number}"
            )

            try:
                self.process_single_pr(pr)
            except github.GithubException as e:
                if e.status == 404:
                    logger.warning(
                        f"PR #{pr.number}: Encountered 404 while processing — skipping."
                    )
                    continue
                raise
            except Exception as e:
                logger.warning(
                    f"PR #{pr.number}: Unexpected error — skipping. Error: {e}"
                )
                continue

            processed += 1

        logger.info(f"Done. Processed {processed} PR(s).")


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="PR State Labeler — manage review-state labels on open PRs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview label changes without applying them",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Cap the number of PRs processed per run (default: no limit)",
    )
    parser.add_argument(
        "--updated-within-hours",
        type=int,
        default=DEFAULT_UPDATED_WITHIN_HOURS,
        metavar="H",
        help="Only check PRs updated in the last H hours; 0 checks every open PR "
        f"(default: {DEFAULT_UPDATED_WITHIN_HOURS})",
    )
    parser.add_argument(
        "--ignore-label-actor",
        action="append",
        default=[],
        metavar="LOGIN",
        help="Don't treat label changes by this login as manual (repeatable)",
    )
    parser.add_argument(
        "--repo",
        default="datahub-project/datahub",
        help="GitHub repository (owner/name) to process (default: datahub-project/datahub)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    logger.info("=" * 60)
    logger.info("PR State Labeler")
    logger.info("=" * 60)
    logger.info(f"Repo    : {args.repo}")
    logger.info(f"Mode    : {'DRY RUN (no changes)' if args.dry_run else 'LIVE'}")
    logger.info(f"Limit   : {args.limit if args.limit else 'none'}")
    logger.info(
        f"Window  : {f'updated in last {args.updated_within_hours}h' if args.updated_within_hours > 0 else 'all open PRs'}"
    )
    logger.info("")

    processor = GithubPRProcessor(
        repo_name=args.repo,
        dry_run=args.dry_run,
        limit=args.limit,
        updated_within_hours=args.updated_within_hours,
        ignored_label_actors=set(args.ignore_label_actor),
    )
    processor.process_prs()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
