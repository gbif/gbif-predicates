package org.gbif.predicate.query;

import java.util.Optional;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

public class OccurrenceEsQueryVisitor extends EsQueryVisitor<OccurrenceSearchParameter> {
  public OccurrenceEsQueryVisitor(EsFieldMapper<OccurrenceSearchParameter> esFieldMapper) {
    super(esFieldMapper);
  }

  @Override
  protected Optional<OccurrenceSearchParameter> getParam(String name) {
    return OccurrenceSearchParameter.lookup(name);
  }
}
