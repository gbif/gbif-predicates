package org.gbif.predicate.query;

import java.util.function.Function;
import org.gbif.dwc.terms.Term;

public abstract class SparkQueryVisitor extends SQLQueryVisitor {

  private static final Function<Term, String> ARRAY_FN =
      t -> "array_contains(" + SQLColumnsUtils.getSQLQueryColumn(t) + ",'%s',%b)";

  @Override
  public Function<Term, String> getArrayFn() {
    return ARRAY_FN;
  }
}
