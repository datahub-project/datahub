package com.linkedin.metadata.search.elasticsearch.query.v3.ranking;

import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import net.objecthunter.exp4j.function.Function;

/** Closed grammar for a template formula. Each variable is a signal contribution. */
public final class RankingFormulaCheck {
  // Signal names may contain '_'. _score and _entityType are rejected as signal names.
  private static final Pattern CHARACTERS = Pattern.compile("[A-Za-z0-9_+*().,_ ]+");
  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private static final Function POW =
      new Function("pow", 2) {
        @Override
        public double apply(double... args) {
          return Math.pow(args[0], args[1]);
        }
      };

  private RankingFormulaCheck() {}

  public static void check(String formula, Set<String> signalNames) {
    if (formula == null || formula.isBlank()) {
      return;
    }
    if (!CHARACTERS.matcher(formula).matches()) {
      throw new SearchTemplateException(
          "Search template formula has a character that is not allowed");
    }
    Matcher identifiers = IDENTIFIER.matcher(formula);
    while (identifiers.find()) {
      String token = identifiers.group();
      int end = identifiers.end();
      boolean call = end < formula.length() && formula.charAt(end) == '(';
      if (call) {
        if (!"pow".equals(token)) {
          throw new SearchTemplateException(
              "Search template formula function is not allowed: " + token);
        }
      } else if (!signalNames.contains(token)) {
        throw new SearchTemplateException(
            "Search template formula names a variable that is not a signal: " + token);
      }
    }
    try {
      ExpressionBuilder builder = new ExpressionBuilder(formula).function(POW);
      for (String name : signalNames) {
        builder.variable(name);
      }
      Expression expression = builder.build();
      if (!signalNames.containsAll(expression.getVariableNames())) {
        throw new SearchTemplateException(
            "Search template formula names a variable that is not a signal");
      }
    } catch (SearchTemplateException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new SearchTemplateException("Search template formula is not valid", e);
    }
  }
}
