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
import javax.validation.constraints.NotNull;
import org.gbif.api.model.common.search.SearchParameter;

/** This predicate checks if its {@code key} is less or equal than its {@code value}. */
public class RangePredicate<S extends SearchParameter> implements Predicate {

  @NotNull private final S key;

  @NotNull private final RangeValue value;

  @JsonCreator
  public RangePredicate(@JsonProperty("key") S key, @JsonProperty("value") RangeValue value) {
    this.key = key;
    this.value = value;
  }

  public S getKey() {
    return key;
  }

  public RangeValue getValue() {
    return value;
  }
}
