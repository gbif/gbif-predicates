package org.gbif.predicate.query;

import java.util.function.Function;
import org.gbif.dwc.terms.Term;

public abstract class HiveQueryVisitor extends SQLQueryVisitor {

  private static final Function<Term, String> ARRAY_FN =
      t -> "stringArrayContains(" + SQLColumnsUtils.getHiveQueryColumn(t) + ",'%s',%b)";

  @Override
  public Function<Term, String> getArrayFn() {
    return ARRAY_FN;
  }
}
