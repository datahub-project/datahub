package datahub.protobuf.visitors.dataset;

import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.url.Url;
import datahub.protobuf.model.ProtobufField;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

public class InstitutionalMemoryVisitor
    implements ProtobufModelVisitor<InstitutionalMemoryMetadata> {
  public static final String TEAM_DESC = "Github Team";
  public static final String SLACK_CHAN_DESC = "Slack Channel";

  private static final Pattern SLACK_CHANNEL_REGEX = Pattern.compile("(?si).*#([a-z0-9-]+).*");
  private static final Pattern LINK_REGEX =
      Pattern.compile(
          "(?s)(\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|])");
  private final String githubOrganization;
  private final Pattern githubTeamRegex;
  private final String slackTeamId;

  public InstitutionalMemoryVisitor(
      @Nullable String slackTeamId, @Nullable String githubOrganization) {
    this.slackTeamId = slackTeamId;
    this.githubOrganization = githubOrganization;
    if (githubOrganization != null) {
      this.githubTeamRegex =
          Pattern.compile(String.format("(?si).*@%s/([a-z-]+).*", githubOrganization));
    } else {
      this.githubTeamRegex = null;
    }
  }

  //  https://slack.com/app_redirect?channel=fdn-analytics-data-catalog&team=T024F4EL1
  private Optional<Url> slackLink(String text) {
    return Optional.ofNullable(slackTeamId)
        .map(
            teamId -> {
              Matcher m = SLACK_CHANNEL_REGEX.matcher(text);
              if (m.matches()) {
                return new Url(
                    String.format(
                        "https://slack.com/app_redirect?channel=%s&team=%s",
                        m.group(1), slackTeamId));
              } else {
                return null;
              }
            });
  }

  private Optional<Url> teamLink(String text) {
    return Optional.ofNullable(githubTeamRegex)
        .map(
            regex -> {
              Matcher m = regex.matcher(text);
              if (m.matches()) {
                return new Url(
                    String.format(
                        "https://github.com/orgs/%s/teams/%s", githubOrganization, m.group(1)));
              } else {
                return null;
              }
            });
  }

  @Override
  public Stream<InstitutionalMemoryMetadata> visitGraph(VisitContext context) {
    List<InstitutionalMemoryMetadata> institutionalMemoryMetadata = new LinkedList<>();

    teamLink(context.root().comment())
        .ifPresent(
            url ->
                institutionalMemoryMetadata.add(
                    new InstitutionalMemoryMetadata()
                        .setCreateStamp(context.getAuditStamp())
                        .setDescription(TEAM_DESC)
                        .setUrl(url)));

    slackLink(context.root().comment())
        .ifPresent(
            url ->
                institutionalMemoryMetadata.add(
                    new InstitutionalMemoryMetadata()
                        .setCreateStamp(context.getAuditStamp())
                        .setDescription(SLACK_CHAN_DESC)
                        .setUrl(url)));

    final int[] cnt = {0};
    MatcherStream.findMatches(LINK_REGEX, context.root().comment())
        .forEach(
            match -> {
              cnt[0] += 1;
              institutionalMemoryMetadata.add(
                  new InstitutionalMemoryMetadata()
                      .setCreateStamp(context.getAuditStamp())
                      .setDescription(
                          String.format("%s Reference %d", context.root().name(), cnt[0]))
                      .setUrl(new Url(match.group(1))));
            });

    return institutionalMemoryMetadata.stream();
  }

  @Override
  public Stream<InstitutionalMemoryMetadata> visitField(ProtobufField field, VisitContext context) {
    List<InstitutionalMemoryMetadata> institutionalMemoryMetadata = new LinkedList<>();

    if (field.messageProto().equals(context.getGraph().root().messageProto())) {
      final int[] cnt = {0};
      MatcherStream.findMatches(LINK_REGEX, field.comment())
          .forEach(
              match -> {
                cnt[0] += 1;
                institutionalMemoryMetadata.add(
                    new InstitutionalMemoryMetadata()
                        .setCreateStamp(context.getAuditStamp())
                        .setDescription(
                            String.format(
                                "%s.%s Reference %d",
                                field.getProtobufMessage().name(),
                                field.getFieldProto().getName(),
                                cnt[0]))
                        .setUrl(new Url(match.group(1))));
              });
    }

    return institutionalMemoryMetadata.stream();
  }

  private static class MatcherStream {
    private MatcherStream() {}

    public static Stream<String> find(Pattern pattern, CharSequence input) {
      return findMatches(pattern, input).map(MatchResult::group);
    }

    public static Stream<MatchResult> findMatches(Pattern pattern, CharSequence input) {
      Matcher matcher = pattern.matcher(input);

      Spliterator<MatchResult> spliterator =
          new Spliterators.AbstractSpliterator<MatchResult>(
              Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL) {
            @Override
            public boolean tryAdvance(Consumer<? super MatchResult> action) {
              if (!matcher.find()) {
                return false;
              }
              action.accept(matcher.toMatchResult());
              return true;
            }
          };

      return StreamSupport.stream(spliterator, false);
    }
  }
}
