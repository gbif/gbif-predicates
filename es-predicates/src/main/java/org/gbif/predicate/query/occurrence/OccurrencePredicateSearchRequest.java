package org.gbif.predicate.query.occurrence;

import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.model.predicate.Predicate;

/** Search request that uses a predicate filter like the ones used un downloads. */
public class OccurrencePredicateSearchRequest extends OccurrenceSearchRequest {

  private Predicate predicate;

  public Predicate getPredicate() {
    return predicate;
  }

  public void setPredicate(Predicate predicate) {
    this.predicate = predicate;
  }
}
