package org.gbif.predicate.query;

import java.util.Optional;
import org.gbif.api.model.event.search.EventSearchParameter;

public class EventEsQueryVisitor extends EsQueryVisitor<EventSearchParameter> {
  public EventEsQueryVisitor(EsFieldMapper<EventSearchParameter> esFieldMapper) {
    super(esFieldMapper);
  }

  @Override
  protected Optional<EventSearchParameter> getParam(String name) {
    return EventSearchParameter.lookupEventParam(name);
  }
}
