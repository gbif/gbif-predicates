package org.gbif.predicate.query;

import java.util.Optional;
import java.util.UUID;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

public class OccurrenceEsQueryVisitor extends EsQueryVisitor<OccurrenceSearchParameter> {
  public OccurrenceEsQueryVisitor(
      EsFieldMapper<OccurrenceSearchParameter> esFieldMapper, UUID defaultChecklistKey) {
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
