package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AiInstructionInput;
import com.linkedin.datahub.graphql.generated.UpdateCorpUserAiAssistantSettingsInput;
import com.linkedin.datahub.graphql.resolvers.settings.ai.AiInstructionValidationUtils;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.AiAssistantSettings;
import com.linkedin.settings.global.AiInstruction;
import com.linkedin.settings.global.AiInstructionArray;
import com.linkedin.settings.global.AiInstructionType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for updating the authenticated user's AI Assistant-specific settings. */
@Slf4j
@RequiredArgsConstructor
public class UpdateCorpUserAiAssistantSettingsResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateCorpUserAiAssistantSettingsInput input =
        bindArgument(
            environment.getArgument("input"), UpdateCorpUserAiAssistantSettingsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            final Urn userUrn = UrnUtils.getUrn(context.getActorUrn());

            final CorpUserSettings maybeSettings =
                _settingsService.getCorpUserSettings(context.getOperationContext(), userUrn);

            final CorpUserSettings newSettings =
                maybeSettings == null
                    ? new CorpUserSettings()
                        .setAppearance(
                            new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
                    : maybeSettings;

            // Patch the new corp user settings. This does a R-M-W.
            updateCorpUserSettings(newSettings, input, context);

            _settingsService.updateCorpUserSettings(
                context.getOperationContext(), userUrn, newSettings);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform user AI assistant settings update against input {}, {}",
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to perform update to user AI assistant settings against input %s",
                    input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static void updateCorpUserSettings(
      @Nonnull final CorpUserSettings settings,
      @Nonnull final UpdateCorpUserAiAssistantSettingsInput input,
      @Nonnull final QueryContext context) {
    final AiAssistantSettings newAiAssistantSettings =
        settings.hasAiAssistant() ? settings.getAiAssistant() : new AiAssistantSettings();
    updateCorpUserAiAssistantSettings(newAiAssistantSettings, input, context);
    settings.setAiAssistant(newAiAssistantSettings);
  }

  private static void updateCorpUserAiAssistantSettings(
      @Nonnull final AiAssistantSettings settings,
      @Nonnull final UpdateCorpUserAiAssistantSettingsInput input,
      @Nonnull final QueryContext context) {

    if (input.getInstructions() != null) {
      // Validate instructions before processing
      AiInstructionValidationUtils.validateAiInstructions(input.getInstructions());

      // Replace entire set of instructions (not append)
      AiInstructionArray instructions = mapAiInstructionInputs(input.getInstructions(), context);
      settings.setInstructions(instructions);
    }
  }

  private static AiInstructionArray mapAiInstructionInputs(
      java.util.List<AiInstructionInput> inputs, @Nonnull final QueryContext context) {
    AiInstructionArray instructions = new AiInstructionArray();
    final long currentTimeMs = System.currentTimeMillis();
    final Urn actor = UrnUtils.getUrn(context.getActorUrn());

    for (AiInstructionInput input : inputs) {
      AiInstruction instruction = new AiInstruction();

      // Set ID - use provided ID or generate UUID
      String id = input.getId();
      if (id == null || id.trim().isEmpty()) {
        id = UUID.randomUUID().toString();
      }
      instruction.setId(id);
      instruction.setType(AiInstructionType.valueOf(input.getType().toString()));

      // Set state - use provided state or default to ACTIVE
      com.linkedin.datahub.graphql.generated.AiInstructionState graphqlState = input.getState();
      com.linkedin.settings.global.AiInstructionState pdlState;
      if (graphqlState == null) {
        pdlState = com.linkedin.settings.global.AiInstructionState.ACTIVE;
      } else {
        pdlState = mapGraphqlStateToPdlState(graphqlState);
      }
      instruction.setState(pdlState);

      // Set instruction text
      instruction.setInstruction(input.getInstruction());

      // Set audit stamps (server-managed)
      AuditStamp auditStamp = new AuditStamp();
      auditStamp.setTime(currentTimeMs);
      auditStamp.setActor(actor);
      instruction.setCreated(auditStamp);
      instruction.setLastModified(auditStamp);

      instructions.add(instruction);
    }
    return instructions;
  }

  private static com.linkedin.settings.global.AiInstructionState mapGraphqlStateToPdlState(
      com.linkedin.datahub.graphql.generated.AiInstructionState graphqlState) {
    switch (graphqlState) {
      case ACTIVE:
        return com.linkedin.settings.global.AiInstructionState.ACTIVE;
      case INACTIVE:
        return com.linkedin.settings.global.AiInstructionState.INACTIVE;
      default:
        throw new IllegalArgumentException("Unknown GraphQL AiInstructionState: " + graphqlState);
    }
  }
}
