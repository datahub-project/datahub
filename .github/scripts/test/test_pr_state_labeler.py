"""Unit tests for pr_state_labeler (run by test-github-scripts.yml: pytest .github/scripts/test)."""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import github
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pr_state_labeler import (  # noqa: E402
    MERGE_PENDING_CI,
    NEEDS_REVIEW,
    PENDING_SUBMITTER_MERGE,
    PENDING_SUBMITTER_RESPONSE,
    GithubPRProcessor,
)

# ============================================================================
# HELPERS
# ============================================================================

T0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
T1 = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
T2 = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
T3 = datetime(2024, 1, 1, 3, 0, 0, tzinfo=timezone.utc)


def make_label(name: str) -> MagicMock:
    label = MagicMock()
    label.name = name
    return label


def make_user(login: str, user_type: str = "User") -> MagicMock:
    user = MagicMock()
    user.login = login
    user.type = user_type
    return user


def make_review(login: str, state: str, submitted_at: datetime) -> MagicMock:
    review = MagicMock()
    review.user = make_user(login)
    review.state = state
    review.submitted_at = submitted_at
    return review


def make_comment(login: str, created_at: datetime, user_type: str = "User") -> MagicMock:
    comment = MagicMock()
    comment.user = make_user(login, user_type)
    comment.created_at = created_at
    return comment


def make_commit(date: datetime, num_parents: int = 1) -> MagicMock:
    commit = MagicMock()
    commit.commit.author.date = date
    commit.parents = [MagicMock()] * num_parents
    return commit


def make_event(
    event_type: str, actor_login: str, created_at: datetime, actor_type: str = "User"
) -> MagicMock:
    event = MagicMock()
    event.event = event_type
    event.actor = make_user(actor_login, actor_type)
    event.created_at = created_at
    return event


def make_pr(
    number: int = 1,
    author: str = "external-user",
    labels: list = None,
    reviews: list = None,
    comments: list = None,
    commits: list = None,
    events: list = None,
) -> MagicMock:
    pr = MagicMock()
    pr.number = number
    pr.user = make_user(author)
    pr.labels = [make_label(l) for l in (labels or [])]
    pr.get_reviews.return_value = reviews or []
    pr.get_issue_comments.return_value = comments or []
    pr.get_commits.return_value = commits or []
    pr.get_issue_events.return_value = events or []
    return pr


def make_processor(
    dry_run: bool = True, collaborators: list = None, **kwargs
) -> GithubPRProcessor:
    """Create a GithubPRProcessor with GitHub client mocked out."""
    collaborators = collaborators or ["reviewer-a", "reviewer-b"]

    with patch.dict(os.environ, {"GITHUB_TOKEN": "fake-token"}):
        with patch("pr_state_labeler.github.Github"):
            processor = GithubPRProcessor(
                repo_name="test-org/test-repo",
                dry_run=dry_run,
                **kwargs,
            )

    # Patch _get_collaborators to return a fixed set (bypass cache + API)
    collab_set = set(collaborators)
    processor._get_collaborators = MagicMock(return_value=collab_set)

    return processor


# ============================================================================
# STATE MACHINE TESTS
# ============================================================================

class TestGetCurrentStateLabel:
    def test_no_state_labels(self):
        assert GithubPRProcessor._get_current_state_label({"area/ingestion", "size/M"}) is None

    def test_single_state_label(self):
        assert GithubPRProcessor._get_current_state_label({NEEDS_REVIEW}) == NEEDS_REVIEW

    def test_precedence_merge_pending_ci_wins(self):
        labels = {NEEDS_REVIEW, PENDING_SUBMITTER_RESPONSE, MERGE_PENDING_CI}
        assert GithubPRProcessor._get_current_state_label(labels) == MERGE_PENDING_CI

    def test_precedence_pending_submitter_merge_over_needs_review(self):
        labels = {NEEDS_REVIEW, PENDING_SUBMITTER_MERGE}
        assert GithubPRProcessor._get_current_state_label(labels) == PENDING_SUBMITTER_MERGE


class TestNoLabels:
    def test_skips_pr_with_no_labels(self):
        """PR with no labels should be skipped (too new)."""
        processor = make_processor()
        pr = make_pr(labels=[])

        processor.process_single_pr(pr)

        pr.add_to_labels.assert_not_called()
        pr.remove_from_labels.assert_not_called()


class TestTerminalStates:
    def test_merge_pending_ci_is_terminal(self):
        """merge-pending-ci should never be overridden."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            labels=[MERGE_PENDING_CI, "area/ingestion"],
            reviews=[make_review("reviewer-a", "APPROVED", T1)],
        )

        processor.process_single_pr(pr)

        pr.add_to_labels.assert_not_called()
        pr.remove_from_labels.assert_not_called()

    def test_pending_submitter_merge_is_terminal(self):
        """pending-submitter-merge should never be overridden."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="reviewer-a",  # internal contributor
            labels=[PENDING_SUBMITTER_MERGE, "area/ingestion"],
            reviews=[make_review("reviewer-b", "APPROVED", T1)],
        )

        processor.process_single_pr(pr)

        pr.add_to_labels.assert_not_called()
        pr.remove_from_labels.assert_not_called()


class TestApproval:
    def test_external_contributor_approval_sets_merge_pending_ci(self):
        """Approved external PR → merge-pending-ci."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",  # not a collaborator
            labels=[NEEDS_REVIEW, "area/ingestion"],
            reviews=[make_review("reviewer-a", "APPROVED", T1)],
        )

        processor.process_single_pr(pr)

        pr.remove_from_labels.assert_called_once_with(NEEDS_REVIEW)
        pr.add_to_labels.assert_called_once_with(MERGE_PENDING_CI)

    def test_internal_contributor_approval_sets_pending_submitter_merge(self):
        """Approved internal PR → pending-submitter-merge."""
        processor = make_processor(
            dry_run=False,
            collaborators=["reviewer-a", "reviewer-b", "internal-author"],
        )
        pr = make_pr(
            author="internal-author",
            labels=[NEEDS_REVIEW, "area/ingestion"],
            reviews=[make_review("reviewer-a", "APPROVED", T1)],
        )

        processor.process_single_pr(pr)

        pr.remove_from_labels.assert_called_once_with(NEEDS_REVIEW)
        pr.add_to_labels.assert_called_once_with(PENDING_SUBMITTER_MERGE)

    def test_changes_requested_blocks_approval(self):
        """APPROVED + CHANGES_REQUESTED = no approval; reviewer activity → pending-submitter-response."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=[NEEDS_REVIEW, "area/ingestion"],
            reviews=[
                make_review("reviewer-a", "APPROVED", T1),
                make_review("reviewer-b", "CHANGES_REQUESTED", T2),
            ],
        )

        processor.process_single_pr(pr)

        # No approval (blocked by CHANGES_REQUESTED), but reviewers have been active
        # → transitions to pending-submitter-response, not merge-pending-ci
        pr.remove_from_labels.assert_called_once_with(NEEDS_REVIEW)
        pr.add_to_labels.assert_called_once_with(PENDING_SUBMITTER_RESPONSE)

    def test_community_contribution_label_overrides_internal(self):
        """Internal author with community-contribution label treated as external."""
        processor = make_processor(
            dry_run=False,
            collaborators=["reviewer-a", "reviewer-b", "internal-author"],
        )
        pr = make_pr(
            author="internal-author",
            labels=[NEEDS_REVIEW, "community-contribution"],
            reviews=[make_review("reviewer-a", "APPROVED", T1)],
        )

        processor.process_single_pr(pr)

        pr.add_to_labels.assert_called_once_with(MERGE_PENDING_CI)


class TestNeedsReview:
    def test_no_reviews_sets_needs_review(self):
        """No approver activity + no manual changes → needs-review."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=["area/ingestion"],  # has area label but no state label
            reviews=[],
            comments=[],
        )

        processor.process_single_pr(pr)

        pr.add_to_labels.assert_called_once_with(NEEDS_REVIEW)

    def test_submitter_commits_after_review_resets_to_needs_review(self):
        """Submitter pushed commits since last review → needs-review."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=[PENDING_SUBMITTER_RESPONSE, "area/ingestion"],
            reviews=[make_review("reviewer-a", "CHANGES_REQUESTED", T1)],
            comments=[],
            commits=[make_commit(T2)],  # commit after review
        )

        processor.process_single_pr(pr)

        pr.remove_from_labels.assert_called_once_with(PENDING_SUBMITTER_RESPONSE)
        pr.add_to_labels.assert_called_once_with(NEEDS_REVIEW)

    def test_submitter_comment_after_review_resets_to_needs_review(self):
        """Submitter left a comment since last review → needs-review."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=[PENDING_SUBMITTER_RESPONSE, "area/ingestion"],
            reviews=[make_review("reviewer-a", "CHANGES_REQUESTED", T1)],
            comments=[make_comment("external-user", T2)],  # comment after review
            commits=[],
        )

        processor.process_single_pr(pr)

        pr.remove_from_labels.assert_called_once_with(PENDING_SUBMITTER_RESPONSE)
        pr.add_to_labels.assert_called_once_with(NEEDS_REVIEW)

    def test_bot_comment_does_not_reset_to_needs_review(self):
        """Bot comments after review should not count as submitter activity."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=[PENDING_SUBMITTER_RESPONSE, "area/ingestion"],
            reviews=[make_review("reviewer-a", "CHANGES_REQUESTED", T1)],
            comments=[make_comment("some-bot", T2, user_type="Bot")],
            commits=[],
        )

        processor.process_single_pr(pr)

        # No reset to needs-review — bot comment doesn't count
        pr.add_to_labels.assert_not_called()

    def test_merge_commit_does_not_count_as_activity(self):
        """Merge commits (2 parents) should be ignored."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=[PENDING_SUBMITTER_RESPONSE, "area/ingestion"],
            reviews=[make_review("reviewer-a", "CHANGES_REQUESTED", T1)],
            comments=[],
            commits=[make_commit(T2, num_parents=2)],  # merge commit
        )

        processor.process_single_pr(pr)

        pr.add_to_labels.assert_not_called()


class TestPendingSubmitterResponse:
    def test_reviewer_reviewed_sets_pending_submitter_response(self):
        """Reviewer left changes requested, submitter hasn't responded → pending-submitter-response."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=[NEEDS_REVIEW, "area/ingestion"],
            reviews=[make_review("reviewer-a", "CHANGES_REQUESTED", T1)],
            comments=[],
            commits=[],
        )

        processor.process_single_pr(pr)

        pr.remove_from_labels.assert_called_once_with(NEEDS_REVIEW)
        pr.add_to_labels.assert_called_once_with(PENDING_SUBMITTER_RESPONSE)


class TestManualLabelOverride:
    def test_manual_label_change_with_no_further_activity_is_respected(self):
        """If a collaborator manually set a label and nothing happened since, skip."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=[PENDING_SUBMITTER_RESPONSE, "area/ingestion"],
            reviews=[],
            comments=[],
            commits=[],
            events=[make_event("labeled", "reviewer-a", T1)],
        )

        processor.process_single_pr(pr)

        pr.add_to_labels.assert_not_called()
        pr.remove_from_labels.assert_not_called()

    def test_manual_label_change_without_state_label_sets_pending_submitter_response(self):
        """Manual label change but no state label → pending-submitter-response."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=["area/ingestion"],  # no state label
            reviews=[],
            comments=[],
            commits=[],
            events=[make_event("labeled", "reviewer-a", T1)],
        )

        processor.process_single_pr(pr)

        pr.add_to_labels.assert_called_once_with(PENDING_SUBMITTER_RESPONSE)


class TestDryRun:
    def test_dry_run_does_not_call_github_api(self):
        """In dry-run mode, no label mutations should be made."""
        processor = make_processor(dry_run=True)
        pr = make_pr(
            author="external-user",
            labels=[NEEDS_REVIEW, "area/ingestion"],
            reviews=[make_review("reviewer-a", "APPROVED", T1)],
        )

        processor.process_single_pr(pr)

        pr.add_to_labels.assert_not_called()
        pr.remove_from_labels.assert_not_called()


class TestProcess404Handling:
    def test_404_on_get_pull_is_skipped(self):
        """A 404 when fetching a PR should be skipped, not fatal."""
        with patch.dict(os.environ, {"GITHUB_TOKEN": "fake-token"}):
            with patch("pr_state_labeler.github.Github") as MockGithub:
                processor = GithubPRProcessor(repo_name="test-org/test-repo", dry_run=True)

        processor._get_collaborators = MagicMock(return_value={"reviewer-a"})

        # Mock search_issues returning one issue
        issue = MagicMock()
        issue.number = 999
        processor.github_client.search_issues.return_value = [issue]

        # Mock get_pull raising 404
        processor.repo.get_pull.side_effect = github.GithubException(
            status=404, data={"message": "Not Found"}, headers={}
        )

        # Should not raise
        processor.process_prs()

        processor.repo.get_pull.assert_called_once_with(999)

    def test_404_during_process_single_pr_is_skipped(self):
        """A 404 raised inside process_single_pr (e.g. get_reviews) should be skipped."""
        with patch.dict(os.environ, {"GITHUB_TOKEN": "fake-token"}):
            with patch("pr_state_labeler.github.Github"):
                processor = GithubPRProcessor(repo_name="test-org/test-repo", dry_run=True)

        processor._get_collaborators = MagicMock(return_value={"reviewer-a"})

        issue = MagicMock()
        issue.number = 42
        processor.github_client.search_issues.return_value = [issue]

        pr = make_pr(number=42, labels=["area/ingestion"])
        processor.repo.get_pull.return_value = pr

        # Simulate get_reviews raising 404
        pr.get_reviews.side_effect = github.GithubException(
            status=404, data={"message": "Not Found"}, headers={}
        )

        processor.process_prs()

        # Processed without raising
        pr.add_to_labels.assert_not_called()

    def test_non_404_github_exception_is_reraised(self):
        """Non-404 GithubException (e.g. 500) should propagate."""
        with patch.dict(os.environ, {"GITHUB_TOKEN": "fake-token"}):
            with patch("pr_state_labeler.github.Github"):
                processor = GithubPRProcessor(repo_name="test-org/test-repo", dry_run=True)

        processor._get_collaborators = MagicMock(return_value={"reviewer-a"})

        issue = MagicMock()
        issue.number = 42
        processor.github_client.search_issues.return_value = [issue]

        processor.repo.get_pull.side_effect = github.GithubException(
            status=500, data={"message": "Internal Server Error"}, headers={}
        )

        with pytest.raises(github.GithubException):
            processor.process_prs()


class TestLimit:
    def test_limit_caps_prs_processed(self):
        """--limit N should stop after N PRs."""
        with patch.dict(os.environ, {"GITHUB_TOKEN": "fake-token"}):
            with patch("pr_state_labeler.github.Github"):
                processor = GithubPRProcessor(
                    repo_name="test-org/test-repo", dry_run=True, limit=2
                )

        processor._get_collaborators = MagicMock(return_value={"reviewer-a"})

        issues = [MagicMock(number=i) for i in range(1, 6)]  # 5 issues
        processor.github_client.search_issues.return_value = issues

        def make_simple_pr(number):
            return make_pr(number=number, labels=["area/ingestion"])

        processor.repo.get_pull.side_effect = lambda n: make_simple_pr(n)

        processor.process_prs()

        assert processor.repo.get_pull.call_count == 2


class TestManualLabelActors:
    def test_bot_label_change_is_not_manual(self):
        """Label changes by bots (e.g. this workflow) don't count as manual overrides."""
        processor = make_processor(dry_run=False)
        pr = make_pr(
            author="external-user",
            labels=["area/ingestion"],
            events=[make_event("labeled", "reviewer-a", T1, actor_type="Bot")],
        )

        processor.process_single_pr(pr)

        # No manual change and no reviews → needs-review
        pr.add_to_labels.assert_called_once_with(NEEDS_REVIEW)

    def test_ignored_actor_label_change_is_not_manual(self):
        """Label changes by an ignored login (old automation account) don't count as manual."""
        processor = make_processor(
            dry_run=False,
            collaborators=["reviewer-a", "old-automation"],
            ignored_label_actors={"old-automation"},
        )
        pr = make_pr(
            author="external-user",
            labels=[PENDING_SUBMITTER_RESPONSE, "area/ingestion"],
            events=[make_event("labeled", "old-automation", T1)],
        )

        processor.process_single_pr(pr)

        # Without the ignore this would be skipped as a manual change.
        pr.remove_from_labels.assert_called_once_with(PENDING_SUBMITTER_RESPONSE)
        pr.add_to_labels.assert_called_once_with(NEEDS_REVIEW)


class TestUpdatedWindow:
    def _search_query(self, **kwargs) -> str:
        processor = make_processor(**kwargs)
        processor.github_client.search_issues.return_value = []
        processor.process_prs()
        return processor.github_client.search_issues.call_args[0][0]

    def test_default_window_filters_by_updated(self):
        assert " updated:>=" in self._search_query()

    def test_zero_window_checks_all_open_prs(self):
        assert "updated:" not in self._search_query(updated_within_hours=0)


class TestCollaboratorsPermission:
    def test_403_listing_collaborators_raises_clear_error(self):
        with patch.dict(os.environ, {"GITHUB_TOKEN": "fake-token"}):
            with patch("pr_state_labeler.github.Github"):
                processor = GithubPRProcessor(repo_name="test-org/test-repo")

        processor.repo.get_collaborators.side_effect = github.GithubException(
            status=403, data={"message": "Forbidden"}, headers={}
        )

        with pytest.raises(RuntimeError, match="cannot list repository collaborators"):
            processor._get_collaborators()
