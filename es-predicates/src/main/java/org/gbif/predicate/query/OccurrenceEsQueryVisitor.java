package org.gbif.predicate.query;

import java.util.Optional;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

public class OccurrenceEsQueryVisitor extends EsQueryVisitor<OccurrenceSearchParameter> {
  public OccurrenceEsQueryVisitor(
      EsFieldMapper<OccurrenceSearchParameter> esFieldMapper, String defaultChecklistKey) {
    super(esFieldMapper, defaultChecklistKey);
  }

  @Override
  protected Optional<OccurrenceSearchParameter> getParam(String name) {
    try {
      return OccurrenceSearchParameter.lookup(name);
    } catch (Exception ex) {
      return Optional.empty();
    }
  }
}
