/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.api.model.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.gbif.api.model.common.search.SearchParameter;

/** This predicate checks if its {@code key} is greater than its {@code value}. */
public class GreaterThanPredicate<S extends SearchParameter> extends SimplePredicate<S> {

  @JsonCreator
  public GreaterThanPredicate(@JsonProperty("key") S key, @JsonProperty("value") String value) {
    super(true, key, value, null);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof GreaterThanPredicate)) {
      return false;
    }

    SimplePredicate<S> that = (SimplePredicate<S>) obj;
    return Objects.equals(this.getKey(), that.getKey())
        && Objects.equals(this.getValue(), that.getValue());
  }
}