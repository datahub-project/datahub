package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.search.template.SearchTemplateLadder;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Walks a template ladder. Selection runs before the query. A shard failure or exception follows
 * {@code next}. The last rung is kept even when its total is zero, and its failure fails the
 * search.
 */
public final class SearchTemplateWalk {
  private SearchTemplateWalk() {}

  public record Request(
      @Nonnull String query,
      @Nullable Filter filter,
      @Nullable List<SortCriterion> sort,
      @Nullable Integer size) {}

  public static final class AttemptResult<T> {
    @Nullable final T page;
    final long total;
    @Nonnull final List<SearchEntity> entities;
    @Nonnull final List<String> highlights;
    final boolean shardFailed;
    @Nullable final RuntimeException error;
    final boolean skipped;

    private AttemptResult(
        @Nullable T page,
        long total,
        @Nonnull List<SearchEntity> entities,
        @Nonnull List<String> highlights,
        boolean shardFailed,
        @Nullable RuntimeException error,
        boolean skipped) {
      this.page = page;
      this.total = total;
      this.entities = entities;
      this.highlights = highlights;
      this.shardFailed = shardFailed;
      this.error = error;
      this.skipped = skipped;
    }

    @Nonnull
    public static <T> AttemptResult<T> ok(
        @Nonnull T page,
        long total,
        @Nonnull List<SearchEntity> entities,
        @Nonnull List<String> highlights,
        boolean shardFailed) {
      return new AttemptResult<>(page, total, entities, highlights, shardFailed, null, false);
    }

    @Nonnull
    public static <T> AttemptResult<T> failed(@Nonnull RuntimeException error) {
      return new AttemptResult<>(null, 0, List.of(), List.of(), false, error, false);
    }

    @Nonnull
    static <T> AttemptResult<T> skipped() {
      return new AttemptResult<>(null, 0, List.of(), List.of(), false, null, true);
    }

    @Nullable
    public T page() {
      return page;
    }

    @Nonnull
    public List<String> highlights() {
      return highlights;
    }

    @Nullable
    public RuntimeException error() {
      return error;
    }
  }

  @FunctionalInterface
  public interface Attempt<T> {
    @Nonnull
    AttemptResult<T> run(@Nonnull String urn);
  }

  @FunctionalInterface
  public interface Fuse<T> {
    @Nonnull
    T fuse(@Nonnull T first, @Nonnull List<SearchEntity> entities, int total);
  }

  public static final class Served<T> {
    @Nonnull private final T page;
    @Nullable private final String urn;
    @Nonnull private final List<String> highlights;

    private Served(@Nonnull T page, @Nullable String urn, @Nonnull List<String> highlights) {
      this.page = page;
      this.urn = urn;
      this.highlights = highlights;
    }

    @Nonnull
    public T page() {
      return page;
    }

    /** Null when the page is a reciprocal-rank fusion and no single rung was served. */
    @Nullable
    public String urn() {
      return urn;
    }

    @Nonnull
    public List<String> highlights() {
      return highlights;
    }
  }

  @Nonnull
  public static <T> Served<T> walk(
      @Nonnull OperationContext opContext,
      @Nonnull SearchTemplateLoader loader,
      @Nonnull ExecutorService executor,
      @Nonnull String urn,
      @Nonnull Request request,
      @Nonnull Attempt<T> attempt,
      @Nonnull Fuse<T> fuse) {
    return walkUrn(opContext, loader, executor, urn, request, attempt, fuse, new HashSet<>(), 0);
  }

  @Nonnull
  private static <T> Served<T> walkUrn(
      @Nonnull OperationContext opContext,
      @Nonnull SearchTemplateLoader loader,
      @Nonnull ExecutorService executor,
      @Nonnull String urn,
      @Nonnull Request request,
      @Nonnull Attempt<T> attempt,
      @Nonnull Fuse<T> fuse,
      @Nonnull Set<String> path,
      int depth) {
    if (!path.add(urn)) {
      throw new SearchTemplateException("Search template ladder cycle at " + urn);
    }
    if (depth > SearchTemplateLoader.MAX_PARENT_DEPTH) {
      throw new SearchTemplateException(
          "Search template ladder exceeds " + SearchTemplateLoader.MAX_PARENT_DEPTH + " at " + urn);
    }
    try {
      MergedSearchTemplate template = loader.load(opContext, urn);
      SearchTemplateLadder ladder = template.getLadder();
      if (ladder != null
          && ladder.hasParallel()
          && ladder.getParallel() != null
          && !ladder.getParallel().isEmpty()) {
        if (!SearchTemplateLadderPredicates.selectMatches(
            ladder, request.query(), request.filter(), request.sort(), request.size())) {
          return follow(
              opContext, loader, executor, ladder, urn, request, attempt, fuse, path, depth);
        }
        return walkParallel(
            opContext, loader, executor, urn, ladder, request, attempt, fuse, path, depth);
      }
      if (ladder != null
          && !SearchTemplateLadderPredicates.selectMatches(
              ladder, request.query(), request.filter(), request.sort(), request.size())) {
        return follow(
            opContext, loader, executor, ladder, urn, request, attempt, fuse, path, depth);
      }
      AttemptResult<T> result = attempt.run(urn);
      if (result.error != null || result.shardFailed) {
        if (ladder == null || !ladder.hasNext() || ladder.getNext() == null) {
          if (result.error != null) {
            throw result.error;
          }
          throw new SearchTemplateException("Search template shard failure on " + urn);
        }
        return follow(
            opContext, loader, executor, ladder, urn, request, attempt, fuse, path, depth);
      }
      boolean keep =
          ladder == null
              || !ladder.hasNext()
              || ladder.getNext() == null
              || SearchTemplateLadderPredicates.stopMatches(ladder, result.total, request.query());
      if (keep) {
        return served(urn, result);
      }
      return follow(opContext, loader, executor, ladder, urn, request, attempt, fuse, path, depth);
    } finally {
      path.remove(urn);
    }
  }

  @Nonnull
  private static <T> Served<T> follow(
      @Nonnull OperationContext opContext,
      @Nonnull SearchTemplateLoader loader,
      @Nonnull ExecutorService executor,
      @Nonnull SearchTemplateLadder ladder,
      @Nonnull String urn,
      @Nonnull Request request,
      @Nonnull Attempt<T> attempt,
      @Nonnull Fuse<T> fuse,
      @Nonnull Set<String> path,
      int depth) {
    if (!ladder.hasNext() || ladder.getNext() == null) {
      throw new SearchTemplateException(
          "Search template " + urn + " was not selected and has no next");
    }
    return walkUrn(
        opContext,
        loader,
        executor,
        ladder.getNext().toString(),
        request,
        attempt,
        fuse,
        path,
        depth + 1);
  }

  @Nonnull
  private static <T> Served<T> walkParallel(
      @Nonnull OperationContext opContext,
      @Nonnull SearchTemplateLoader loader,
      @Nonnull ExecutorService executor,
      @Nonnull String groupUrn,
      @Nonnull SearchTemplateLadder ladder,
      @Nonnull Request request,
      @Nonnull Attempt<T> attempt,
      @Nonnull Fuse<T> fuse,
      @Nonnull Set<String> path,
      int depth) {
    SearchTemplateLadderJoins join = SearchTemplateLadderJoins.parse(ladder.getJoin());
    if (join == SearchTemplateLadderJoins.MERGE) {
      throw new SearchTemplateException(
          "Search template merge join is not supported until fusion is implemented");
    }
    List<Urn> members = new ArrayList<>(ladder.getParallel());
    List<Future<Member<T>>> futures = new ArrayList<>();
    for (Urn member : members) {
      String memberUrn = member.toString();
      if (path.contains(memberUrn)) {
        throw new SearchTemplateException("Search template ladder cycle at " + memberUrn);
      }
      futures.add(
          executor.submit(
              () ->
                  runMember(opContext, loader, memberUrn, request, attempt, new HashSet<>(path))));
    }
    List<Member<T>> finished = new ArrayList<>();
    RuntimeException lastError = null;
    boolean shard = false;
    if (join == SearchTemplateLadderJoins.ANY) {
      Member<T> winner = firstToStop(futures);
      cancel(futures);
      if (winner != null) {
        return served(winner.urn, winner.result);
      }
      for (Future<Member<T>> future : futures) {
        if (!future.isCancelled() && future.isDone()) {
          Member<T> member = join(future);
          finished.add(member);
          if (member.result.error != null) {
            lastError = member.result.error;
          }
          shard = shard || member.result.shardFailed;
        }
      }
    } else {
      for (Future<Member<T>> future : futures) {
        Member<T> member = join(future);
        finished.add(member);
        if (member.result.error != null) {
          lastError = member.result.error;
        }
        shard = shard || member.result.shardFailed;
      }
    }
    if (join == SearchTemplateLadderJoins.ALL_EMPTY) {
      for (Member<T> member : finished) {
        if (member.stopped && member.result.page != null) {
          return served(member.urn, member.result);
        }
      }
    }
    if (ladder.hasNext() && ladder.getNext() != null) {
      return walkUrn(
          opContext,
          loader,
          executor,
          ladder.getNext().toString(),
          request,
          attempt,
          fuse,
          path,
          depth + 1);
    }
    if (lastError != null) {
      throw lastError;
    }
    if (shard) {
      throw new SearchTemplateException("Search template shard failure on " + groupUrn);
    }
    throw new SearchTemplateException(
        "Search template " + groupUrn + " was not selected and has no next");
  }

  @Nullable
  private static <T> Member<T> firstToStop(@Nonnull List<Future<Member<T>>> futures) {
    List<Future<Member<T>>> pending = new ArrayList<>(futures);
    while (!pending.isEmpty()) {
      Member<T> earliest = null;
      Future<Member<T>> earliestFuture = null;
      for (Future<Member<T>> future : pending) {
        if (!future.isDone()) {
          continue;
        }
        Member<T> member = join(future);
        if (earliest == null || member.completedAt < earliest.completedAt) {
          earliest = member;
          earliestFuture = future;
        }
      }
      if (earliest == null) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SearchTemplateException("Search template ladder was interrupted", e);
        }
        continue;
      }
      pending.remove(earliestFuture);
      if (earliest.stopped && earliest.result.page != null) {
        return earliest;
      }
    }
    return null;
  }

  @Nonnull
  private static <T> Member<T> runMember(
      @Nonnull OperationContext opContext,
      @Nonnull SearchTemplateLoader loader,
      @Nonnull String urn,
      @Nonnull Request request,
      @Nonnull Attempt<T> attempt,
      @Nonnull Set<String> path) {
    if (Thread.currentThread().isInterrupted()) {
      return new Member<>(urn, AttemptResult.skipped(), false);
    }
    path.add(urn);
    MergedSearchTemplate template = loader.load(opContext, urn);
    SearchTemplateLadder ladder = template.getLadder();
    if (ladder != null
        && !SearchTemplateLadderPredicates.selectMatches(
            ladder, request.query(), request.filter(), request.sort(), request.size())) {
      return new Member<>(urn, AttemptResult.skipped(), false);
    }
    AttemptResult<T> result;
    try {
      result = attempt.run(urn);
    } catch (RuntimeException e) {
      result = AttemptResult.failed(e);
    }
    boolean stopped =
        result.page != null
            && result.error == null
            && !result.shardFailed
            && SearchTemplateLadderPredicates.stopMatches(ladder, result.total, request.query());
    return new Member<>(urn, result, stopped);
  }

  @Nonnull
  private static <T> Served<T> served(@Nonnull String urn, @Nonnull AttemptResult<T> result) {
    if (result.page == null) {
      throw new SearchTemplateException("Search template produced no page");
    }
    return new Served<>(result.page, urn, result.highlights);
  }

  private static void cancel(@Nonnull List<? extends Future<?>> futures) {
    for (Future<?> future : futures) {
      future.cancel(true);
    }
  }

  @Nonnull
  private static <T> Member<T> join(@Nonnull Future<Member<T>> future) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SearchTemplateException("Search template ladder was interrupted", e);
    } catch (Exception e) {
      RuntimeException cause = e.getCause() instanceof RuntimeException runtime ? runtime : null;
      if (cause instanceof SearchTemplateException template) {
        throw template;
      }
      if (cause != null) {
        throw cause;
      }
      throw new SearchTemplateException("Search template ladder failed", e);
    }
  }

  private static final class Member<T> {
    @Nonnull final String urn;
    @Nonnull final AttemptResult<T> result;
    final boolean stopped;
    final long completedAt = System.nanoTime();

    private Member(@Nonnull String urn, @Nonnull AttemptResult<T> result, boolean stopped) {
      this.urn = urn;
      this.result = result;
      this.stopped = stopped;
    }
  }
}
