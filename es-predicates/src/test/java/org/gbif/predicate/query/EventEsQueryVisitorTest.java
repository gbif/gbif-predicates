/*
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
package org.gbif.predicate.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import org.gbif.api.exception.QueryBuildingException;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.GeoDistancePredicate;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.IsNotNullPredicate;
import org.gbif.api.model.predicate.IsNullPredicate;
import org.gbif.api.model.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.predicate.LessThanPredicate;
import org.gbif.api.model.predicate.LikePredicate;
import org.gbif.api.model.predicate.NotPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.predicate.RangePredicate;
import org.gbif.api.model.predicate.WithinPredicate;
import org.gbif.api.util.RangeValue;
import org.junit.jupiter.api.Test;

/** Test cases for the Elasticsearch query visitor. */
public class EventEsQueryVisitorTest {

  private static final EventSearchParameter PARAM = EventSearchParameter.ISLAND;
  private static final EventSearchParameter PARAM2 = EventSearchParameter.INSTITUTION_CODE;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final EsFieldMapper<EventSearchParameter> fieldMapper = new EventEsFieldMapperTest();
  private final EventEsQueryVisitor visitor =
      new EventEsQueryVisitor(fieldMapper, "defaultChecklistKey");

  private void assertQueryEquals(String expectedJson, String actualQuery)
      throws JsonProcessingException {
    JsonNode expected = objectMapper.readTree(expectedJson.trim());
    JsonNode actual = objectMapper.readTree(actualQuery);
    assertEquals(expected, actual);
  }

  @Test
  public void testEqualsPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new EqualsPredicate<>(PARAM, "value", false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"term\": {\n"
            + "                  \"island.keyword\": {\n"
            + "                    \"value\": \"value\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsPredicateMatchVerbatim()
      throws QueryBuildingException, JsonProcessingException {
    Predicate p = new EqualsPredicate<>(PARAM, "value", true);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"term\": {\n"
            + "                  \"island.verbatim\": {\n"
            + "                    \"value\": \"value\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsDatePredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new EqualsPredicate<>(EventSearchParameter.EVENT_DATE, "2021-09-16", false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"event_date\": {\n"
            + "                    \"gte\": \"2021-09-16\",\n"
            + "                    \"lt\": \"2021-09-17\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    // An InPredicate should be exactly the same
    p = new InPredicate<>(EventSearchParameter.EVENT_DATE, Arrays.asList("2021-09-16"), false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"should\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"range\": {\n"
            + "                        \"event_date\": {\n"
            + "                          \"gte\": \"2021-09-16\",\n"
            + "                          \"lt\": \"2021-09-17\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p =
        new InPredicate<>(
            EventSearchParameter.EVENT_DATE, Arrays.asList("2021-09-16", "2024-01-17"), false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"should\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"range\": {\n"
            + "                        \"event_date\": {\n"
            + "                          \"gte\": \"2021-09-16\",\n"
            + "                          \"lt\": \"2021-09-17\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"range\": {\n"
            + "                        \"event_date\": {\n"
            + "                          \"gte\": \"2024-01-17\",\n"
            + "                          \"lt\": \"2024-01-18\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsRangePredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new EqualsPredicate<>(EventSearchParameter.ELEVATION, "-20.0,600", false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"elevation\": {\n"
            + "                    \"gte\": -20.0,\n"
            + "                    \"lte\": 600.0\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new EqualsPredicate<>(EventSearchParameter.ELEVATION, "*,600", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"elevation\": {\n"
            + "                    \"lte\": 600.0\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new EqualsPredicate<>(EventSearchParameter.ELEVATION, "-20.0,*", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"elevation\": {\n"
            + "                    \"gte\": -20.0\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsDateRangePredicate()
      throws QueryBuildingException, JsonProcessingException {
    // Occurrences will be returned if the occurrence date/date range is
    // *completely within* the query date or date range.
    Predicate p =
        new EqualsPredicate<>(EventSearchParameter.EVENT_DATE, "1980-02,2021-09-16", false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"event_date\": {\n"
            + "                    \"gte\": \"1980-02-01\",\n"
            + "                    \"lt\": \"2021-09-17\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new EqualsPredicate<>(EventSearchParameter.EVENT_DATE, "1980", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"event_date\": {\n"
            + "                    \"gte\": \"1980-01-01\",\n"
            + "                    \"lt\": \"1981-01-01\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new EqualsPredicate<>(EventSearchParameter.EVENT_DATE, "1980,1990-05-06", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"event_date\": {\n"
            + "                    \"gte\": \"1980-01-01\",\n"
            + "                    \"lt\": \"1990-05-07\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new EqualsPredicate<>(EventSearchParameter.EVENT_DATE, "1990-05-06", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"event_date\": {\n"
            + "                    \"gte\": \"1990-05-06\",\n"
            + "                    \"lt\": \"1990-05-07\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testGreaterThanOrEqualPredicate()
      throws QueryBuildingException, JsonProcessingException {
    Predicate p = new GreaterThanOrEqualsPredicate<>(EventSearchParameter.ELEVATION, "222");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"elevation\": {\n"
            + "                    \"gte\": \"222\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new GreaterThanOrEqualsPredicate<>(EventSearchParameter.LAST_INTERPRETED, "2021-09-16");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"last_interpreted\": {\n"
            + "                    \"gte\": \"2021-09-16\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new GreaterThanOrEqualsPredicate<>(EventSearchParameter.LAST_INTERPRETED, "2021");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"last_interpreted\": {\n"
            + "                    \"gte\": \"2021\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new GreaterThanPredicate<>(EventSearchParameter.ELEVATION, "1000");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"elevation\": {\n"
            + "                    \"gt\": \"1000\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new GreaterThanPredicate<>(EventSearchParameter.LAST_INTERPRETED, "2021-09-16");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"last_interpreted\": {\n"
            + "                    \"gt\": \"2021-09-16\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new GreaterThanPredicate<>(EventSearchParameter.LAST_INTERPRETED, "2021");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"last_interpreted\": {\n"
            + "                    \"gt\": \"2021\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testLessThanOrEqualPredicate()
      throws QueryBuildingException, JsonProcessingException {
    Predicate p = new LessThanOrEqualsPredicate<>(EventSearchParameter.ELEVATION, "1000");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"elevation\": {\n"
            + "                    \"lte\": \"1000\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new LessThanOrEqualsPredicate<>(EventSearchParameter.LAST_INTERPRETED, "2021-10-25");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"last_interpreted\": {\n"
            + "                    \"lte\": \"2021-10-25\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new LessThanOrEqualsPredicate<>(EventSearchParameter.LAST_INTERPRETED, "2021");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"last_interpreted\": {\n"
            + "                    \"lte\": \"2021\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new LessThanPredicate<>(EventSearchParameter.ELEVATION, "1000");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"elevation\": {\n"
            + "                    \"lt\": \"1000\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new LessThanPredicate<>(EventSearchParameter.LAST_INTERPRETED, "2021-10-25");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"last_interpreted\": {\n"
            + "                    \"lt\": \"2021-10-25\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);

    p = new LessThanPredicate<>(EventSearchParameter.LAST_INTERPRETED, "2021");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"last_interpreted\": {\n"
            + "                    \"lt\": \"2021\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p3 = new GreaterThanOrEqualsPredicate<>(EventSearchParameter.MONTH, "12");
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"term\": {\n"
            + "                        \"island.keyword\": {\n"
            + "                          \"value\": \"value_1\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"term\": {\n"
            + "                        \"institution_code.keyword\": {\n"
            + "                          \"value\": \"value_2\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"range\": {\n"
            + "                        \"month\": {\n"
            + "                          \"gte\": \"12\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p3 = new EqualsPredicate<>(PARAM, "value_3", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"should\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"term\": {\n"
            + "                        \"institution_code.keyword\": {\n"
            + "                          \"value\": \"value_2\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"terms\": {\n"
            + "                  \"island.keyword\": [\"value_3\", \"value_1\"]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testDisjunctionMatchCasePredicate()
      throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM, "value_2", false);

    Predicate p3 = new EqualsPredicate<>(PARAM, "value_3", true);
    Predicate p4 = new EqualsPredicate<>(PARAM, "value_4", true);

    DisjunctionPredicate p = new DisjunctionPredicate(Arrays.asList(p1, p2, p3, p4));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"should\": [\n"
            + "              {\n"
            + "                \"terms\": {\n"
            + "                  \"island.keyword\": [\"value_2\", \"value_1\"]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"terms\": {\n"
            + "                  \"island.verbatim\": [\"value_4\", \"value_3\"]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testInPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new InPredicate<>(PARAM, Arrays.asList("value_1", "value_2", "value_3"), false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"terms\": {\n"
            + "                  \"island.keyword\": [\"value_1\", \"value_2\", \"value_3\"]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testComplexInPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new InPredicate<>(PARAM, Arrays.asList("value_1", "value_2", "value_3"), false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"term\": {\n"
            + "                        \"island.keyword\": {\n"
            + "                          \"value\": \"value_1\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"terms\": {\n"
            + "                        \"island.keyword\": [\"value_1\", \"value_2\", \"value_3\"]\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"term\": {\n"
            + "                        \"institution_code.keyword\": {\n"
            + "                          \"value\": \"value_2\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new NotPredicate(new EqualsPredicate<>(PARAM, "value", false));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"must_not\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"term\": {\n"
            + "                        \"island.keyword\": {\n"
            + "                          \"value\": \"value\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);

    ConjunctionPredicate cp = new ConjunctionPredicate(Arrays.asList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"must_not\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"term\": {\n"
            + "                              \"island.keyword\": {\n"
            + "                                \"value\": \"value_1\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    },\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"term\": {\n"
            + "                              \"institution_code.keyword\": {\n"
            + "                                \"value\": \"value_2\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testLikePredicate() throws QueryBuildingException, JsonProcessingException {
    // NB: ? and * are wildcards (as in ES).  SQL-like _ and % are literal.
    LikePredicate<EventSearchParameter> likePredicate =
        new LikePredicate<>(PARAM, "v?l*ue_%", false);
    String query = visitor.buildQuery(likePredicate);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"wildcard\": {\n"
            + "                  \"island.keyword\": {\n"
            + "                    \"value\": \"v?l*ue_%\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testLikeVerbatimPredicate() throws QueryBuildingException, JsonProcessingException {
    LikePredicate<EventSearchParameter> likePredicate =
        new LikePredicate<>(PARAM, "v?l*ue_%", true);
    String query = visitor.buildQuery(likePredicate);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"wildcard\": {\n"
            + "                  \"island.verbatim\": {\n"
            + "                    \"value\": \"v?l*ue_%\"\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testComplexLikePredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"term\": {\n"
            + "                        \"island.keyword\": {\n"
            + "                          \"value\": \"value_1\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"wildcard\": {\n"
            + "                        \"island.keyword\": {\n"
            + "                          \"value\": \"value_1*\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"term\": {\n"
            + "                        \"institution_code.keyword\": {\n"
            + "                          \"value\": \"value_2\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new IsNotNullPredicate<>(PARAM);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"exists\": {\n"
            + "                  \"field\": \"island.keyword\"\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testIsNullPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new IsNullPredicate<>(PARAM);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"must_not\": [\n"
            + "                    {\n"
            + "                      \"exists\": {\n"
            + "                        \"field\": \"island.keyword\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testWithinPredicate() throws QueryBuildingException, JsonProcessingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.buildQuery(p);
    assertNotNull(query);
  }

  @Test
  public void testGeoDistancePredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p = new GeoDistancePredicate("10", "20", "10km");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"geo_distance\": {\n"
            + "                  \"coordinates\": {\n"
            + "                    \"lat\": 10.0,\n"
            + "                    \"lon\": 20.0\n"
            + "                  },\n"
            + "                  \"distance\": \"10.0km\"\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testComplexPredicateOne() throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate pcon = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    Predicate pdis = new DisjunctionPredicate(Arrays.asList(p1, pcon));
    Predicate p = new NotPredicate(pdis);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"must_not\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"should\": [\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"term\": {\n"
            + "                              \"island.keyword\": {\n"
            + "                                \"value\": \"value_1\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    },\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"bool\": {\n"
            + "                              \"filter\": [\n"
            + "                                {\n"
            + "                                  \"term\": {\n"
            + "                                    \"island.keyword\": {\n"
            + "                                      \"value\": \"value_1\"\n"
            + "                                    }\n"
            + "                                  }\n"
            + "                                }\n"
            + "                              ]\n"
            + "                            }\n"
            + "                          },\n"
            + "                          {\n"
            + "                            \"bool\": {\n"
            + "                              \"filter\": [\n"
            + "                                {\n"
            + "                                  \"wildcard\": {\n"
            + "                                    \"island.keyword\": {\n"
            + "                                      \"value\": \"value_1*\"\n"
            + "                                    }\n"
            + "                                  }\n"
            + "                                }\n"
            + "                              ]\n"
            + "                            }\n"
            + "                          },\n"
            + "                          {\n"
            + "                            \"bool\": {\n"
            + "                              \"filter\": [\n"
            + "                                {\n"
            + "                                  \"term\": {\n"
            + "                                    \"institution_code.keyword\": {\n"
            + "                                      \"value\": \"value_2\"\n"
            + "                                    }\n"
            + "                                  }\n"
            + "                                }\n"
            + "                              ]\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testComplexPredicateTwo() throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);

    Predicate p4 = new DisjunctionPredicate(Arrays.asList(p1, p3));
    Predicate p5 = new ConjunctionPredicate(Arrays.asList(p1, p2));

    Predicate p = new ConjunctionPredicate(Arrays.asList(p4, new NotPredicate(p5)));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"should\": [\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"term\": {\n"
            + "                              \"island.keyword\": {\n"
            + "                                \"value\": \"value_1\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    },\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"term\": {\n"
            + "                              \"institution_code.keyword\": {\n"
            + "                                \"value\": \"value_2\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"must_not\": [\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"bool\": {\n"
            + "                              \"filter\": [\n"
            + "                                {\n"
            + "                                  \"term\": {\n"
            + "                                    \"island.keyword\": {\n"
            + "                                      \"value\": \"value_1\"\n"
            + "                                    }\n"
            + "                                  }\n"
            + "                                }\n"
            + "                              ]\n"
            + "                            }\n"
            + "                          },\n"
            + "                          {\n"
            + "                            \"bool\": {\n"
            + "                              \"filter\": [\n"
            + "                                {\n"
            + "                                  \"wildcard\": {\n"
            + "                                    \"island.keyword\": {\n"
            + "                                      \"value\": \"value_1*\"\n"
            + "                                    }\n"
            + "                                  }\n"
            + "                                }\n"
            + "                              ]\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testComplexPredicateThree() throws QueryBuildingException, JsonProcessingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";

    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p4 = new WithinPredicate(wkt);

    Predicate p5 = new DisjunctionPredicate(Arrays.asList(p1, p3, p4));
    Predicate p6 = new ConjunctionPredicate(Arrays.asList(p1, p2));

    Predicate p = new ConjunctionPredicate(Arrays.asList(p5, p6));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"should\": [\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"term\": {\n"
            + "                              \"island.keyword\": {\n"
            + "                                \"value\": \"value_1\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    },\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"term\": {\n"
            + "                              \"institution_code.keyword\": {\n"
            + "                                \"value\": \"value_2\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    },\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"geo_shape\": {\n"
            + "                              \"scoordinates\": {\n"
            + "                                \"shape\": {\n"
            + "                                  \"coordinates\": [[[30.0, 10.0], [10.0, 20.0], [20.0, 40.0], [40.0, 40.0], [30.0, 10.0]]],\n"
            + "                                  \"type\": \"Polygon\"\n"
            + "                                },\n"
            + "                                \"relation\": \"within\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"term\": {\n"
            + "                              \"island.keyword\": {\n"
            + "                                \"value\": \"value_1\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    },\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"wildcard\": {\n"
            + "                              \"island.keyword\": {\n"
            + "                                \"value\": \"value_1*\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testVocabularyEqualsPredicate() {

    Arrays.stream(EventSearchParameter.values())
        .filter(fieldMapper::isVocabulary)
        .forEach(
            param -> {
              try {
                Predicate p = new EqualsPredicate<>(param, "value", false);
                String searchFieldName = fieldMapper.getExactMatchFieldName(param);
                String query = visitor.buildQuery(p);
                String expectedQuery =
                    ("{\n"
                        + "                      \"bool\": {\n"
                        + "                        \"filter\": [\n"
                        + "                          {\n"
                        + "                            \"nested\": {\n"
                        + "                              \"path\": \"event.humboldt\",\n"
                        + "                              \"query\": {\n"
                        + "                                \"term\": {\n"
                        + "                                  \""
                        + searchFieldName
                        + "\": {\n"
                        + "                                    \"value\": \"value\"\n"
                        + "                                  }\n"
                        + "                                }\n"
                        + "                              },\n"
                        + "                              \"score_mode\": \"none\"\n"
                        + "                            }\n"
                        + "                          }\n"
                        + "                        ]\n"
                        + "                      }\n"
                        + "                    }\n");
                assertQueryEquals(expectedQuery, query);
              } catch (QueryBuildingException | JsonProcessingException ex) {
                throw new RuntimeException(ex);
              }
            });
  }

  @Test
  public void testIntInclusiveRangeWithRangePredicate()
      throws QueryBuildingException, JsonProcessingException {

    RangeValue rangeValue = new RangeValue("1990", null, "2011", null);
    Predicate p = new RangePredicate(EventSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"year\": {\n"
            + "                    \"gte\": 1990,\n"
            + "                    \"lte\": 2011\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testIntExclusiveRangeWithRangePredicate()
      throws QueryBuildingException, JsonProcessingException {

    RangeValue rangeValue = new RangeValue(null, "1990", null, "2011");
    Predicate p = new RangePredicate(EventSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"year\": {\n"
            + "                    \"gt\": 1990,\n"
            + "                    \"lt\": 2011\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testInclusiveExclusiveRangeWithRangePredicate()
      throws QueryBuildingException, JsonProcessingException {

    RangeValue rangeValue = new RangeValue("1990", null, null, "2011");
    Predicate p = new RangePredicate(EventSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"year\": {\n"
            + "                    \"gte\": 1990,\n"
            + "                    \"lt\": 2011\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testExclusiveInclusiveRangeWithRangePredicate()
      throws QueryBuildingException, JsonProcessingException {

    RangeValue rangeValue = new RangeValue(null, "1990", "2011", null);
    Predicate p = new RangePredicate(EventSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"range\": {\n"
            + "                  \"year\": {\n"
            + "                    \"gt\": 1990,\n"
            + "                    \"lte\": 2011\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  /** Test a single taxon key query with a checklist key specified. */
  @Test
  public void testMultiTaxonomyPredicate() throws JsonProcessingException {
    EqualsPredicate equalsPredicate =
        new EqualsPredicate<EventSearchParameter>(
            EventSearchParameter.TAXON_KEY,
            "urn:lsid:marinespecies.org:taxname:368663",
            false,
            "2d59e5db-57ad-41ff-97d6-11f5fb264527");
    try {
      String query = visitor.buildQuery(equalsPredicate);
      String expectedQuery =
          "{\n"
              + "            \"bool\": {\n"
              + "              \"filter\": [\n"
              + "                {\n"
              + "                  \"term\": {\n"
              + "                    \"classifications.2d59e5db-57ad-41ff-97d6-11f5fb264527.taxonKeys\": {\n"
              + "                      \"value\": \"urn:lsid:marinespecies.org:taxname:368663\"\n"
              + "                    }\n"
              + "                  }\n"
              + "                }\n"
              + "              ]\n"
              + "            }\n"
              + "          }\n";
      assertQueryEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  /** Test a single taxon key query with a checklist key specified. */
  @Test
  public void testMultiTaxonomyPredicates() throws JsonProcessingException {
    InPredicate inPredicate =
        new InPredicate<EventSearchParameter>(
            EventSearchParameter.TAXON_KEY,
            List.of(
                "urn:lsid:marinespecies.org:taxname:368663",
                "urn:lsid:marinespecies.org:taxname:368664"),
            false,
            "2d59e5db-57ad-41ff-97d6-11f5fb264527");
    try {
      String query = visitor.buildQuery(inPredicate);
      String expectedQuery =
          "{\n"
              + "            \"bool\": {\n"
              + "              \"filter\": [\n"
              + "                {\n"
              + "                  \"terms\": {\n"
              + "                    \"classifications.2d59e5db-57ad-41ff-97d6-11f5fb264527.taxonKeys\": [\n"
              + "                      \"urn:lsid:marinespecies.org:taxname:368663\",\n"
              + "                      \"urn:lsid:marinespecies.org:taxname:368664\"\n"
              + "                    ]\n"
              + "                  }\n"
              + "                }\n"
              + "              ]\n"
              + "            }\n"
              + "          }\n";
      assertQueryEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  /**
   * Tests queries with 2 taxon keys coming from 2 separate classification with conjunction
   * predicate (AND).
   */
  @Test
  public void testMultipleTaxonomiesConjunctionPredicate() throws JsonProcessingException {

    ConjunctionPredicate predicate =
        new ConjunctionPredicate(
            Arrays.asList(
                new EqualsPredicate<>(
                    EventSearchParameter.TAXON_KEY,
                    "urn:lsid:marinespecies.org:taxname:1",
                    false,
                    "checklistkey1"),
                new EqualsPredicate<>(
                    EventSearchParameter.TAXON_KEY,
                    "urn:lsid:marinespecies.org:taxname:2",
                    false,
                    "checklistkey2")));
    try {
      String query = visitor.buildQuery(predicate);
      String expectedQuery =
          "{\n"
              + "            \"bool\": {\n"
              + "              \"filter\": [\n"
              + "                {\n"
              + "                  \"bool\": {\n"
              + "                    \"filter\": [\n"
              + "                      {\n"
              + "                        \"term\": {\n"
              + "                          \"classifications.checklistkey1.taxonKeys\": {\n"
              + "                            \"value\": \"urn:lsid:marinespecies.org:taxname:1\"\n"
              + "                          }\n"
              + "                        }\n"
              + "                      }\n"
              + "                    ]\n"
              + "                  }\n"
              + "                },\n"
              + "                {\n"
              + "                  \"bool\": {\n"
              + "                    \"filter\": [\n"
              + "                      {\n"
              + "                        \"term\": {\n"
              + "                          \"classifications.checklistkey2.taxonKeys\": {\n"
              + "                            \"value\": \"urn:lsid:marinespecies.org:taxname:2\"\n"
              + "                          }\n"
              + "                        }\n"
              + "                      }\n"
              + "                    ]\n"
              + "                  }\n"
              + "                }\n"
              + "              ]\n"
              + "            }\n"
              + "          }\n";
      assertQueryEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  /**
   * Tests queries with 2 taxon keys coming from 2 separate classification with disjunction
   * predicate (OR).
   */
  @Test
  public void testMultipleTaxonomiesDisjunctionPredicate() throws JsonProcessingException {

    DisjunctionPredicate predicate =
        new DisjunctionPredicate(
            Arrays.asList(
                new EqualsPredicate<>(
                    EventSearchParameter.TAXON_KEY,
                    "urn:lsid:marinespecies.org:taxname:1",
                    false,
                    "checklistkey1"),
                new EqualsPredicate<>(
                    EventSearchParameter.TAXON_KEY,
                    "urn:lsid:marinespecies.org:taxname:2",
                    false,
                    "checklistkey2")));
    try {
      String query = visitor.buildQuery(predicate);
      String expectedQuery =
          "{\n"
              + "            \"bool\": {\n"
              + "              \"should\": [\n"
              + "                {\n"
              + "                  \"bool\": {\n"
              + "                    \"filter\": [\n"
              + "                      {\n"
              + "                        \"term\": {\n"
              + "                          \"classifications.checklistkey1.taxonKeys\": {\n"
              + "                            \"value\": \"urn:lsid:marinespecies.org:taxname:1\"\n"
              + "                          }\n"
              + "                        }\n"
              + "                      }\n"
              + "                    ]\n"
              + "                  }\n"
              + "                },\n"
              + "                {\n"
              + "                  \"bool\": {\n"
              + "                    \"filter\": [\n"
              + "                      {\n"
              + "                        \"term\": {\n"
              + "                          \"classifications.checklistkey2.taxonKeys\": {\n"
              + "                            \"value\": \"urn:lsid:marinespecies.org:taxname:2\"\n"
              + "                          }\n"
              + "                        }\n"
              + "                      }\n"
              + "                    ]\n"
              + "                  }\n"
              + "                }\n"
              + "              ]\n"
              + "            }\n"
              + "          }\n";
      assertQueryEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testIsNotNullTaxonKeyPredicate() throws JsonProcessingException {

    DisjunctionPredicate predicate =
        new DisjunctionPredicate(
            Arrays.asList(
                new IsNotNullPredicate<>(EventSearchParameter.TAXON_KEY, "test-checklist-key"),
                new IsNullPredicate<>(EventSearchParameter.SCIENTIFIC_NAME, "test-checklist-key")));
    try {
      String query = visitor.buildQuery(predicate);
      String expectedQuery =
          "{\n"
              + "            \"bool\": {\n"
              + "              \"should\": [\n"
              + "                {\n"
              + "                  \"bool\": {\n"
              + "                    \"filter\": [\n"
              + "                      {\n"
              + "                        \"exists\": {\n"
              + "                          \"field\": \"classifications.test-checklist-key.taxonKeys\"\n"
              + "                        }\n"
              + "                      }\n"
              + "                    ]\n"
              + "                  }\n"
              + "                },\n"
              + "                {\n"
              + "                  \"bool\": {\n"
              + "                    \"filter\": [\n"
              + "                      {\n"
              + "                        \"bool\": {\n"
              + "                          \"must_not\": [\n"
              + "                            {\n"
              + "                              \"exists\": {\n"
              + "                                \"field\": \"classifications.test-checklist-key.usage.name\"\n"
              + "                              }\n"
              + "                            }\n"
              + "                          ]\n"
              + "                        }\n"
              + "                      }\n"
              + "                    ]\n"
              + "                  }\n"
              + "                }\n"
              + "              ]\n"
              + "            }\n"
              + "          }\n";
      assertQueryEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testLikeScientificNamePredicateWithChecklist() throws JsonProcessingException {

    LikePredicate predicate =
        new LikePredicate<>(
            EventSearchParameter.SCIENTIFIC_NAME, "Acacia", "test-checklist-key", false);
    try {
      String query = visitor.buildQuery(predicate);
      String expectedQuery =
          "{\n"
              + "            \"bool\": {\n"
              + "              \"filter\": [\n"
              + "                {\n"
              + "                  \"wildcard\": {\n"
              + "                    \"classifications.test-checklist-key.usage.name\": {\n"
              + "                      \"value\": \"Acacia\"\n"
              + "                    }\n"
              + "                  }\n"
              + "                }\n"
              + "              ]\n"
              + "            }\n"
              + "          }\n";
      assertQueryEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testYearRange() throws JsonProcessingException {

    EqualsPredicate predicate = new EqualsPredicate<>(EventSearchParameter.YEAR, "1900,*", false);
    try {
      String query = visitor.buildQuery(predicate);
      String expectedQuery =
          "{\n"
              + "            \"bool\": {\n"
              + "              \"filter\": [\n"
              + "                {\n"
              + "                  \"range\": {\n"
              + "                    \"year\": {\n"
              + "                      \"gte\": 1900.0\n"
              + "                    }\n"
              + "                  }\n"
              + "                }\n"
              + "              ]\n"
              + "            }\n"
              + "          }\n";
      assertQueryEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testYearRangeReversed() throws JsonProcessingException {

    EqualsPredicate predicate = new EqualsPredicate<>(EventSearchParameter.YEAR, "*,1900", false);
    try {
      String query = visitor.buildQuery(predicate);
      String expectedQuery =
          "{\n"
              + "            \"bool\": {\n"
              + "              \"filter\": [\n"
              + "                {\n"
              + "                  \"range\": {\n"
              + "                    \"year\": {\n"
              + "                      \"lte\": 1900.0\n"
              + "                    }\n"
              + "                  }\n"
              + "                }\n"
              + "              ]\n"
              + "            }\n"
              + "          }\n";
      assertQueryEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testTwoYearRanges() throws JsonProcessingException {

    DisjunctionPredicate conjunctionPredicate =
        new DisjunctionPredicate(
            Arrays.asList(
                new EqualsPredicate<>(EventSearchParameter.YEAR, "1900,*", false),
                new EqualsPredicate<>(EventSearchParameter.YEAR, "2000,*", false)));
    try {
      String query = visitor.buildQuery(conjunctionPredicate);
      String expectedQuery =
          "{\n"
              + "            \"bool\": {\n"
              + "              \"should\": [\n"
              + "                {\n"
              + "                  \"bool\": {\n"
              + "                    \"filter\": [\n"
              + "                      {\n"
              + "                        \"range\": {\n"
              + "                          \"year\": {\n"
              + "                            \"gte\": 1900.0\n"
              + "                          }\n"
              + "                        }\n"
              + "                      }\n"
              + "                    ]\n"
              + "                  }\n"
              + "                },\n"
              + "                {\n"
              + "                  \"bool\": {\n"
              + "                    \"filter\": [\n"
              + "                      {\n"
              + "                        \"range\": {\n"
              + "                          \"year\": {\n"
              + "                            \"gte\": 2000.0\n"
              + "                          }\n"
              + "                        }\n"
              + "                      }\n"
              + "                    ]\n"
              + "                  }\n"
              + "                }\n"
              + "              ]\n"
              + "            }\n"
              + "          }\n";
      assertQueryEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testEqualsNestedPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p =
        new EqualsPredicate<>(EventSearchParameter.HUMBOLDT_COMPILATION_TYPES, "value", false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"nested\": {\n"
            + "                  \"path\": \"event.humboldt\",\n"
            + "                  \"query\": {\n"
            + "                    \"term\": {\n"
            + "                      \"humboldt_compilation_types.keyword\": {\n"
            + "                        \"value\": \"value\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  },\n"
            + "                  \"score_mode\": \"none\"\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testConjunctionNestedMixedPredicate()
      throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 =
        new EqualsPredicate<>(EventSearchParameter.HUMBOLDT_COMPILATION_TYPES, "value_2", false);
    Predicate p3 = new GreaterThanOrEqualsPredicate<>(EventSearchParameter.MONTH, "12");
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"term\": {\n"
            + "                        \"island.keyword\": {\n"
            + "                          \"value\": \"value_1\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"filter\": [\n"
            + "                    {\n"
            + "                      \"range\": {\n"
            + "                        \"month\": {\n"
            + "                          \"gte\": \"12\"\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"nested\": {\n"
            + "                  \"path\": \"event.humboldt\",\n"
            + "                  \"query\": {\n"
            + "                    \"bool\": {\n"
            + "                      \"filter\": [\n"
            + "                        {\n"
            + "                          \"term\": {\n"
            + "                            \"humboldt_compilation_types.keyword\": {\n"
            + "                              \"value\": \"value_2\"\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ]\n"
            + "                    }\n"
            + "                  },\n"
            + "                  \"score_mode\": \"none\"\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testComplexNestedPredicate() throws QueryBuildingException, JsonProcessingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 =
        new LikePredicate<>(EventSearchParameter.HUMBOLDT_COMPILATION_TYPES, "value_1*", false);
    Predicate p3 =
        new EqualsPredicate<>(EventSearchParameter.HUMBOLDT_PROTOCOL_NAMES, "value_2", false);

    Predicate p4 = new DisjunctionPredicate(Arrays.asList(p1, p3));
    Predicate p5 = new ConjunctionPredicate(Arrays.asList(p1, p2));

    Predicate p = new ConjunctionPredicate(Arrays.asList(p4, new NotPredicate(p5)));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"should\": [\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"term\": {\n"
            + "                              \"island.keyword\": {\n"
            + "                                \"value\": \"value_1\"\n"
            + "                              }\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    },\n"
            + "                    {\n"
            + "                      \"nested\": {\n"
            + "                        \"path\": \"event.humboldt\",\n"
            + "                        \"query\": {\n"
            + "                          \"bool\": {\n"
            + "                            \"should\": [\n"
            + "                              {\n"
            + "                                \"term\": {\n"
            + "                                  \"humboldt_protocol_names.keyword\": {\n"
            + "                                    \"value\": \"value_2\"\n"
            + "                                  }\n"
            + "                                }\n"
            + "                              }\n"
            + "                            ]\n"
            + "                          }\n"
            + "                        },\n"
            + "                        \"score_mode\": \"none\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"bool\": {\n"
            + "                  \"must_not\": [\n"
            + "                    {\n"
            + "                      \"bool\": {\n"
            + "                        \"filter\": [\n"
            + "                          {\n"
            + "                            \"bool\": {\n"
            + "                              \"filter\": [\n"
            + "                                {\n"
            + "                                  \"term\": {\n"
            + "                                    \"island.keyword\": {\n"
            + "                                      \"value\": \"value_1\"\n"
            + "                                    }\n"
            + "                                  }\n"
            + "                                }\n"
            + "                              ]\n"
            + "                            }\n"
            + "                          },\n"
            + "                          {\n"
            + "                            \"nested\": {\n"
            + "                              \"path\": \"event.humboldt\",\n"
            + "                              \"query\": {\n"
            + "                                \"bool\": {\n"
            + "                                  \"filter\": [\n"
            + "                                    {\n"
            + "                                      \"wildcard\": {\n"
            + "                                        \"humboldt_compilation_types.keyword\": {\n"
            + "                                          \"value\": \"value_1*\"\n"
            + "                                        }\n"
            + "                                      }\n"
            + "                                    }\n"
            + "                                  ]\n"
            + "                                }\n"
            + "                              },\n"
            + "                              \"score_mode\": \"none\"\n"
            + "                            }\n"
            + "                          }\n"
            + "                        ]\n"
            + "                      }\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }

  @Test
  public void testConjunctionNestedPredicate()
      throws QueryBuildingException, JsonProcessingException {
    Predicate p1 =
        new EqualsPredicate<>(EventSearchParameter.HUMBOLDT_VERBATIM_SITE_NAMES, "value_1", false);
    Predicate p2 =
        new EqualsPredicate<>(EventSearchParameter.HUMBOLDT_PROTOCOL_NAMES, "value_2", false);
    Predicate p3 = new ConjunctionPredicate(Arrays.asList(p1, p2));
    String query = visitor.buildQuery(p3);
    String expectedQuery =
        "{\n"
            + "          \"bool\": {\n"
            + "            \"filter\": [\n"
            + "              {\n"
            + "                \"nested\": {\n"
            + "                  \"path\": \"event.humboldt\",\n"
            + "                  \"query\": {\n"
            + "                    \"bool\": {\n"
            + "                      \"filter\": [\n"
            + "                        {\n"
            + "                          \"term\": {\n"
            + "                            \"humboldt_verbatim_site_names.keyword\": {\n"
            + "                              \"value\": \"value_1\"\n"
            + "                            }\n"
            + "                          }\n"
            + "                        },\n"
            + "                        {\n"
            + "                          \"term\": {\n"
            + "                            \"humboldt_protocol_names.keyword\": {\n"
            + "                              \"value\": \"value_2\"\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ]\n"
            + "                    }\n"
            + "                  },\n"
            + "                  \"score_mode\": \"none\"\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n";
    assertQueryEquals(expectedQuery, query);
  }
}
