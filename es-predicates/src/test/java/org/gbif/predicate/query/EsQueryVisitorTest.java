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

import java.util.Arrays;
import java.util.List;
import org.gbif.api.exception.QueryBuildingException;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
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
public class EsQueryVisitorTest {

  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 =
      OccurrenceSearchParameter.INSTITUTION_CODE;

  private final EsFieldMapper<OccurrenceSearchParameter> fieldMapper =
      new OccurrenceEsFieldMapperTest();
  private final EsQueryVisitor<OccurrenceSearchParameter> visitor =
      new EsQueryVisitor<>(fieldMapper);

  @Test
  public void testEqualsPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(PARAM, "value", false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"term\" : {\n"
            + "          \"catalog_number.keyword\" : {\n"
            + "            \"value\" : \"value\",\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsPredicateMatchVerbatim() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(PARAM, "value", true);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"term\" : {\n"
            + "          \"catalog_number.verbatim\" : {\n"
            + "            \"value\" : \"value\",\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsDatePredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2021-09-16", false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"event_date\" : {\n"
            + "            \"from\" : \"2021-09-16\",\n"
            + "            \"to\" : \"2021-09-17\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"relation\" : \"within\",\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    // An InPredicate should be exactly the same
    p = new InPredicate<>(OccurrenceSearchParameter.EVENT_DATE, Arrays.asList("2021-09-16"), false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"range\" : {\n"
            + "                \"event_date\" : {\n"
            + "                  \"from\" : \"2021-09-16\",\n"
            + "                  \"to\" : \"2021-09-17\",\n"
            + "                  \"include_lower\" : true,\n"
            + "                  \"include_upper\" : false,\n"
            + "                  \"relation\" : \"within\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p =
        new InPredicate<>(
            OccurrenceSearchParameter.EVENT_DATE, Arrays.asList("2021-09-16", "2024-01-17"), false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"range\" : {\n"
            + "                \"event_date\" : {\n"
            + "                  \"from\" : \"2021-09-16\",\n"
            + "                  \"to\" : \"2021-09-17\",\n"
            + "                  \"include_lower\" : true,\n"
            + "                  \"include_upper\" : false,\n"
            + "                  \"relation\" : \"within\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"range\" : {\n"
            + "                \"event_date\" : {\n"
            + "                  \"from\" : \"2024-01-17\",\n"
            + "                  \"to\" : \"2024-01-18\",\n"
            + "                  \"include_lower\" : true,\n"
            + "                  \"include_upper\" : false,\n"
            + "                  \"relation\" : \"within\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsRangePredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "-20.0,600", false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : -20.0,\n"
            + "            \"to\" : 600.0,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "*,600", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : 600.0,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "-20.0,*", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : -20.0,\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsDateRangePredicate() throws QueryBuildingException {
    // Occurrences will be returned if the occurrence date/date range is
    // *completely within* the query date or date range.
    Predicate p =
        new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "1980-02,2021-09-16", false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"event_date\" : {\n"
            + "            \"from\" : \"1980-02-01\",\n"
            + "            \"to\" : \"2021-09-17\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"relation\" : \"within\",\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "1980", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"event_date\" : {\n"
            + "            \"from\" : \"1980-01-01\",\n"
            + "            \"to\" : \"1981-01-01\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"relation\" : \"within\",\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "1980,1990-05-06", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"event_date\" : {\n"
            + "            \"from\" : \"1980-01-01\",\n"
            + "            \"to\" : \"1990-05-07\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"relation\" : \"within\",\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "1990-05-06", false);
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"event_date\" : {\n"
            + "            \"from\" : \"1990-05-06\",\n"
            + "            \"to\" : \"1990-05-07\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"relation\" : \"within\",\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    System.out.println(expectedQuery);
    System.out.println(expectedQuery.replace(" ", "").replace("\n", ""));
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testGreaterThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "222");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : \"222\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p =
        new GreaterThanOrEqualsPredicate<>(
            OccurrenceSearchParameter.LAST_INTERPRETED, "2021-09-16");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"last_interpreted\" : {\n"
            + "            \"from\" : \"2021-09-16\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"last_interpreted\" : {\n"
            + "            \"from\" : \"2021\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanPredicate<>(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : \"1000\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : false,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new GreaterThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021-09-16");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"last_interpreted\" : {\n"
            + "            \"from\" : \"2021-09-16\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : false,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new GreaterThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"last_interpreted\" : {\n"
            + "            \"from\" : \"2021\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : false,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testLessThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"1000\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021-10-25");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"last_interpreted\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"2021-10-25\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"last_interpreted\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"2021\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException {
    Predicate p = new LessThanPredicate<>(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"1000\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new LessThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021-10-25");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"last_interpreted\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"2021-10-25\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);

    p = new LessThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021");
    query = visitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"last_interpreted\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"2021\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p3 = new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.MONTH, "12");
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"catalog_number.keyword\" : {\n"
            + "                  \"value\" : \"value_1\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"institution_code.keyword\" : {\n"
            + "                  \"value\" : \"value_2\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"range\" : {\n"
            + "                \"month\" : {\n"
            + "                  \"from\" : \"12\",\n"
            + "                  \"to\" : null,\n"
            + "                  \"include_lower\" : true,\n"
            + "                  \"include_upper\" : true,\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p3 = new EqualsPredicate<>(PARAM, "value_3", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"institution_code.keyword\" : {\n"
            + "                  \"value\" : \"value_2\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"catalog_number.keyword\" : [\n"
            + "            \"value_3\",\n"
            + "            \"value_1\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testDisjunctionMatchCasePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM, "value_2", false);

    Predicate p3 = new EqualsPredicate<>(PARAM, "value_3", true);
    Predicate p4 = new EqualsPredicate<>(PARAM, "value_4", true);

    DisjunctionPredicate p = new DisjunctionPredicate(Arrays.asList(p1, p2, p3, p4));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"catalog_number.keyword\" : [\n"
            + "            \"value_2\",\n"
            + "            \"value_1\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"catalog_number.verbatim\" : [\n"
            + "            \"value_4\",\n"
            + "            \"value_3\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testInPredicate() throws QueryBuildingException {
    Predicate p = new InPredicate<>(PARAM, Arrays.asList("value_1", "value_2", "value_3"), false);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"catalog_number.keyword\" : [\n"
            + "            \"value_1\",\n"
            + "            \"value_2\",\n"
            + "            \"value_3\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testComplexInPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new InPredicate<>(PARAM, Arrays.asList("value_1", "value_2", "value_3"), false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"catalog_number.keyword\" : {\n"
            + "                  \"value\" : \"value_1\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"terms\" : {\n"
            + "                \"catalog_number.keyword\" : [\n"
            + "                  \"value_1\",\n"
            + "                  \"value_2\",\n"
            + "                  \"value_3\"\n"
            + "                ],\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"institution_code.keyword\" : {\n"
            + "                  \"value\" : \"value_2\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException {
    Predicate p = new NotPredicate(new EqualsPredicate<>(PARAM, "value", false));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must_not\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"catalog_number.keyword\" : {\n"
            + "                  \"value\" : \"value\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);

    ConjunctionPredicate cp = new ConjunctionPredicate(Arrays.asList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must_not\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalog_number.keyword\" : {\n"
            + "                        \"value\" : \"value_1\",\n"
            + "                        \"boost\" : 1.0\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"institution_code.keyword\" : {\n"
            + "                        \"value\" : \"value_2\",\n"
            + "                        \"boost\" : 1.0\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testLikePredicate() throws QueryBuildingException {
    // NB: ? and * are wildcards (as in ES).  SQL-like _ and % are literal.
    LikePredicate<OccurrenceSearchParameter> likePredicate =
        new LikePredicate<>(PARAM, "v?l*ue_%", false);
    String query = visitor.buildQuery(likePredicate);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"catalog_number.keyword\" : {\n"
            + "            \"wildcard\" : \"v?l*ue_%\",\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testLikeVerbatimPredicate() throws QueryBuildingException {
    LikePredicate<OccurrenceSearchParameter> likePredicate =
        new LikePredicate<>(PARAM, "v?l*ue_%", true);
    String query = visitor.buildQuery(likePredicate);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"catalog_number.verbatim\" : {\n"
            + "            \"wildcard\" : \"v?l*ue_%\",\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testComplexLikePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"catalog_number.keyword\" : {\n"
            + "                  \"value\" : \"value_1\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"wildcard\" : {\n"
            + "                \"catalog_number.keyword\" : {\n"
            + "                  \"wildcard\" : \"value_1*\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"institution_code.keyword\" : {\n"
            + "                  \"value\" : \"value_2\",\n"
            + "                  \"boost\" : 1.0\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate<>(PARAM);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"exists\" : {\n"
            + "          \"field\" : \"catalog_number.keyword\",\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testIsNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNullPredicate<>(PARAM);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"must_not\" : [\n"
            + "            {\n"
            + "              \"exists\" : {\n"
            + "                \"field\" : \"catalog_number.keyword\",\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.buildQuery(p);
    assertNotNull(query);
  }

  @Test
  public void testGeoDistancePredicate() throws QueryBuildingException {
    Predicate p = new GeoDistancePredicate("10", "20", "10km");
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"geo_distance\" : {\n"
            + "          \"coordinates\" : [\n"
            + "            20.0,\n"
            + "            10.0\n"
            + "          ],\n"
            + "          \"distance\" : 10000.0,\n"
            + "          \"distance_type\" : \"arc\",\n"
            + "          \"validation_method\" : \"STRICT\",\n"
            + "          \"ignore_unmapped\" : false,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testComplexPredicateOne() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate pcon = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    Predicate pdis = new DisjunctionPredicate(Arrays.asList(p1, pcon));
    Predicate p = new NotPredicate(pdis);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must_not\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalog_number.keyword\" : {\n"
            + "                        \"value\" : \"value_1\",\n"
            + "                        \"boost\" : 1.0\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"term\" : {\n"
            + "                            \"catalog_number.keyword\" : {\n"
            + "                              \"value\" : \"value_1\",\n"
            + "                              \"boost\" : 1.0\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"wildcard\" : {\n"
            + "                            \"catalog_number.keyword\" : {\n"
            + "                              \"wildcard\" : \"value_1*\",\n"
            + "                              \"boost\" : 1.0\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"term\" : {\n"
            + "                            \"institution_code.keyword\" : {\n"
            + "                              \"value\" : \"value_2\",\n"
            + "                              \"boost\" : 1.0\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testComplexPredicateTwo() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);

    Predicate p4 = new DisjunctionPredicate(Arrays.asList(p1, p3));
    Predicate p5 = new ConjunctionPredicate(Arrays.asList(p1, p2));

    Predicate p = new ConjunctionPredicate(Arrays.asList(p4, new NotPredicate(p5)));
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalog_number.keyword\" : {\n"
            + "                        \"value\" : \"value_1\",\n"
            + "                        \"boost\" : 1.0\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"institution_code.keyword\" : {\n"
            + "                        \"value\" : \"value_2\",\n"
            + "                        \"boost\" : 1.0\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"must_not\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"term\" : {\n"
            + "                            \"catalog_number.keyword\" : {\n"
            + "                              \"value\" : \"value_1\",\n"
            + "                              \"boost\" : 1.0\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"wildcard\" : {\n"
            + "                            \"catalog_number.keyword\" : {\n"
            + "                              \"wildcard\" : \"value_1*\",\n"
            + "                              \"boost\" : 1.0\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testComplexPredicateThree() throws QueryBuildingException {
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
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalog_number.keyword\" : {\n"
            + "                        \"value\" : \"value_1\",\n"
            + "                        \"boost\" : 1.0\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"institution_code.keyword\" : {\n"
            + "                        \"value\" : \"value_2\",\n"
            + "                        \"boost\" : 1.0\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"geo_shape\" : {\n"
            + "                      \"scoordinates\" : {\n"
            + "                        \"shape\" : {\n"
            + "                          \"type\" : \"Polygon\",\n"
            + "                          \"coordinates\" : [\n"
            + "                            [\n"
            + "                              [\n"
            + "                                30.0,\n"
            + "                                10.0\n"
            + "                              ],\n"
            + "                              [\n"
            + "                                10.0,\n"
            + "                                20.0\n"
            + "                              ],\n"
            + "                              [\n"
            + "                                20.0,\n"
            + "                                40.0\n"
            + "                              ],\n"
            + "                              [\n"
            + "                                40.0,\n"
            + "                                40.0\n"
            + "                              ],\n"
            + "                              [\n"
            + "                                30.0,\n"
            + "                                10.0\n"
            + "                              ]\n"
            + "                            ]\n"
            + "                          ]\n"
            + "                        },\n"
            + "                        \"relation\" : \"within\"\n"
            + "                      },\n"
            + "                      \"ignore_unmapped\" : false,\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalog_number.keyword\" : {\n"
            + "                        \"value\" : \"value_1\",\n"
            + "                        \"boost\" : 1.0\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"wildcard\" : {\n"
            + "                      \"catalog_number.keyword\" : {\n"
            + "                        \"wildcard\" : \"value_1*\",\n"
            + "                        \"boost\" : 1.0\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testVocabularyEqualsPredicate() {

    Arrays.stream(OccurrenceSearchParameter.values())
        .filter(fieldMapper::isVocabulary)
        .forEach(
            param -> {
              try {
                Predicate p = new EqualsPredicate<>(param, "value", false);
                String searchFieldName = fieldMapper.getExactMatchFieldName(param);
                String query = visitor.buildQuery(p);
                String expectedQuery =
                    "{\n"
                        + "  \"bool\" : {\n"
                        + "    \"filter\" : [\n"
                        + "      {\n"
                        + "        \"term\" : {\n"
                        + "          \""
                        + searchFieldName
                        + "\" : {\n"
                        + "            \"value\" : \"value\",\n"
                        + "            \"boost\" : 1.0\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ],\n"
                        + "    \"adjust_pure_negative\" : true,\n"
                        + "    \"boost\" : 1.0\n"
                        + "  }\n"
                        + "}";
                assertEquals(expectedQuery, query);
              } catch (QueryBuildingException ex) {
                throw new RuntimeException(ex);
              }
            });
  }

  @Test
  public void testIntInclusiveRangeWithRangePredicate() throws QueryBuildingException {

    RangeValue rangeValue = new RangeValue("1990", null, "2011", null);
    Predicate p = new RangePredicate(OccurrenceSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"year\" : {\n"
            + "            \"from\" : 1990,\n"
            + "            \"to\" : 2011,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testIntExclusiveRangeWithRangePredicate() throws QueryBuildingException {

    RangeValue rangeValue = new RangeValue(null, "1990", null, "2011");
    Predicate p = new RangePredicate(OccurrenceSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"year\" : {\n"
            + "            \"from\" : 1990,\n"
            + "            \"to\" : 2011,\n"
            + "            \"include_lower\" : false,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testInclusiveExclusiveRangeWithRangePredicate() throws QueryBuildingException {

    RangeValue rangeValue = new RangeValue("1990", null, null, "2011");
    Predicate p = new RangePredicate(OccurrenceSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"year\" : {\n"
            + "            \"from\" : 1990,\n"
            + "            \"to\" : 2011,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testExclusiveInclusiveRangeWithRangePredicate() throws QueryBuildingException {

    RangeValue rangeValue = new RangeValue(null, "1990", "2011", null);
    Predicate p = new RangePredicate(OccurrenceSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"year\" : {\n"
            + "            \"from\" : 1990,\n"
            + "            \"to\" : 2011,\n"
            + "            \"include_lower\" : false,\n"
            + "            \"include_upper\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testGreaterThanEqualsIncludingNull() {
    GreaterThanOrEqualsPredicate<OccurrenceSearchParameter> distanceFromCentroidPredicate =
        new GreaterThanOrEqualsPredicate<>(
            OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS, "10");
    try {
      String query = visitor.buildQuery(distanceFromCentroidPredicate);
      assertEquals(
          "{\n"
              + "  \"bool\" : {\n"
              + "    \"filter\" : [\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"should\" : [\n"
              + "            {\n"
              + "              \"range\" : {\n"
              + "                \"distance_from_centroid_in_meters\" : {\n"
              + "                  \"from\" : \"10\",\n"
              + "                  \"to\" : null,\n"
              + "                  \"include_lower\" : true,\n"
              + "                  \"include_upper\" : true,\n"
              + "                  \"boost\" : 1.0\n"
              + "                }\n"
              + "              }\n"
              + "            },\n"
              + "            {\n"
              + "              \"bool\" : {\n"
              + "                \"must_not\" : [\n"
              + "                  {\n"
              + "                    \"exists\" : {\n"
              + "                      \"field\" : \"distance_from_centroid_in_meters\",\n"
              + "                      \"boost\" : 1.0\n"
              + "                    }\n"
              + "                  }\n"
              + "                ],\n"
              + "                \"adjust_pure_negative\" : true,\n"
              + "                \"boost\" : 1.0\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      }\n"
              + "    ],\n"
              + "    \"adjust_pure_negative\" : true,\n"
              + "    \"boost\" : 1.0\n"
              + "  }\n"
              + "}",
          query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testGreaterThanIncludingNull() {
    GreaterThanPredicate<OccurrenceSearchParameter> distanceFromCentroidPredicate =
        new GreaterThanPredicate<>(
            OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS, "10");
    try {
      String query = visitor.buildQuery(distanceFromCentroidPredicate);
      assertEquals(
          "{\n"
              + "  \"bool\" : {\n"
              + "    \"filter\" : [\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"should\" : [\n"
              + "            {\n"
              + "              \"range\" : {\n"
              + "                \"distance_from_centroid_in_meters\" : {\n"
              + "                  \"from\" : \"10\",\n"
              + "                  \"to\" : null,\n"
              + "                  \"include_lower\" : false,\n"
              + "                  \"include_upper\" : true,\n"
              + "                  \"boost\" : 1.0\n"
              + "                }\n"
              + "              }\n"
              + "            },\n"
              + "            {\n"
              + "              \"bool\" : {\n"
              + "                \"must_not\" : [\n"
              + "                  {\n"
              + "                    \"exists\" : {\n"
              + "                      \"field\" : \"distance_from_centroid_in_meters\",\n"
              + "                      \"boost\" : 1.0\n"
              + "                    }\n"
              + "                  }\n"
              + "                ],\n"
              + "                \"adjust_pure_negative\" : true,\n"
              + "                \"boost\" : 1.0\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      }\n"
              + "    ],\n"
              + "    \"adjust_pure_negative\" : true,\n"
              + "    \"boost\" : 1.0\n"
              + "  }\n"
              + "}",
          query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testDisjunctionGreaterThanEqualsIncludingNull() {
    GreaterThanOrEqualsPredicate<OccurrenceSearchParameter> distanceFromCentroidPredicate =
        new GreaterThanOrEqualsPredicate<>(
            OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS, "10");
    EqualsPredicate<OccurrenceSearchParameter> equalsPredicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "6", false);
    DisjunctionPredicate disjunctionPredicate =
        new DisjunctionPredicate(Arrays.asList(equalsPredicate, distanceFromCentroidPredicate));
    try {
      String query = visitor.buildQuery(disjunctionPredicate);
      assertEquals(
          "{\n"
              + "  \"bool\" : {\n"
              + "    \"should\" : [\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"filter\" : [\n"
              + "            {\n"
              + "              \"term\" : {\n"
              + "                \"classifications.defaultChecklistKey.taxonKeys\" : {\n"
              + "                  \"value\" : \"6\",\n"
              + "                  \"boost\" : 1.0\n"
              + "                }\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      },\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"should\" : [\n"
              + "            {\n"
              + "              \"bool\" : {\n"
              + "                \"filter\" : [\n"
              + "                  {\n"
              + "                    \"bool\" : {\n"
              + "                      \"should\" : [\n"
              + "                        {\n"
              + "                          \"range\" : {\n"
              + "                            \"distance_from_centroid_in_meters\" : {\n"
              + "                              \"from\" : \"10\",\n"
              + "                              \"to\" : null,\n"
              + "                              \"include_lower\" : true,\n"
              + "                              \"include_upper\" : true,\n"
              + "                              \"boost\" : 1.0\n"
              + "                            }\n"
              + "                          }\n"
              + "                        },\n"
              + "                        {\n"
              + "                          \"bool\" : {\n"
              + "                            \"must_not\" : [\n"
              + "                              {\n"
              + "                                \"exists\" : {\n"
              + "                                  \"field\" : \"distance_from_centroid_in_meters\",\n"
              + "                                  \"boost\" : 1.0\n"
              + "                                }\n"
              + "                              }\n"
              + "                            ],\n"
              + "                            \"adjust_pure_negative\" : true,\n"
              + "                            \"boost\" : 1.0\n"
              + "                          }\n"
              + "                        }\n"
              + "                      ],\n"
              + "                      \"adjust_pure_negative\" : true,\n"
              + "                      \"boost\" : 1.0\n"
              + "                    }\n"
              + "                  }\n"
              + "                ],\n"
              + "                \"adjust_pure_negative\" : true,\n"
              + "                \"boost\" : 1.0\n"
              + "              }\n"
              + "            },\n"
              + "            {\n"
              + "              \"bool\" : {\n"
              + "                \"must_not\" : [\n"
              + "                  {\n"
              + "                    \"exists\" : {\n"
              + "                      \"field\" : \"distance_from_centroid_in_meters\",\n"
              + "                      \"boost\" : 1.0\n"
              + "                    }\n"
              + "                  }\n"
              + "                ],\n"
              + "                \"adjust_pure_negative\" : true,\n"
              + "                \"boost\" : 1.0\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      }\n"
              + "    ],\n"
              + "    \"adjust_pure_negative\" : true,\n"
              + "    \"boost\" : 1.0\n"
              + "  }\n"
              + "}",
          query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  /** Test a single taxon key query with a checklist key specified. */
  @Test
  public void testMultiTaxonomyPredicate() {
    EqualsPredicate equalsPredicate =
        new EqualsPredicate<OccurrenceSearchParameter>(
            OccurrenceSearchParameter.TAXON_KEY,
            "urn:lsid:marinespecies.org:taxname:368663",
            false,
            "2d59e5db-57ad-41ff-97d6-11f5fb264527");
    try {
      String query = visitor.buildQuery(equalsPredicate);
      String expectedQuery =
          "{\n"
              + "  \"bool\" : {\n"
              + "    \"filter\" : [\n"
              + "      {\n"
              + "        \"term\" : {\n"
              + "          \"classifications.2d59e5db-57ad-41ff-97d6-11f5fb264527.taxonKeys\" : {\n"
              + "            \"value\" : \"urn:lsid:marinespecies.org:taxname:368663\",\n"
              + "            \"boost\" : 1.0\n"
              + "          }\n"
              + "        }\n"
              + "      }\n"
              + "    ],\n"
              + "    \"adjust_pure_negative\" : true,\n"
              + "    \"boost\" : 1.0\n"
              + "  }\n"
              + "}";
      assertEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  /** Test a single taxon key query with a checklist key specified. */
  @Test
  public void testMultiTaxonomyPredicates() {
    InPredicate inPredicate =
        new InPredicate<OccurrenceSearchParameter>(
            OccurrenceSearchParameter.TAXON_KEY,
            List.of(
                "urn:lsid:marinespecies.org:taxname:368663",
                "urn:lsid:marinespecies.org:taxname:368664"),
            false,
            "2d59e5db-57ad-41ff-97d6-11f5fb264527");
    try {
      String query = visitor.buildQuery(inPredicate);
      String expectedQuery =
          "{\n"
              + "  \"bool\" : {\n"
              + "    \"filter\" : [\n"
              + "      {\n"
              + "        \"terms\" : {\n"
              + "          \"classifications.2d59e5db-57ad-41ff-97d6-11f5fb264527.taxonKeys\" : [\n"
              + "            \"urn:lsid:marinespecies.org:taxname:368663\",\n"
              + "            \"urn:lsid:marinespecies.org:taxname:368664\"\n"
              + "          ],\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      }\n"
              + "    ],\n"
              + "    \"adjust_pure_negative\" : true,\n"
              + "    \"boost\" : 1.0\n"
              + "  }\n"
              + "}";
      assertEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  /**
   * Tests queries with 2 taxon keys coming from 2 separate classification with conjunction
   * predicate (AND).
   */
  @Test
  public void testMultipleTaxonomiesConjunctionPredicate() {

    ConjunctionPredicate predicate =
        new ConjunctionPredicate(
            Arrays.asList(
                new EqualsPredicate<>(
                    OccurrenceSearchParameter.TAXON_KEY,
                    "urn:lsid:marinespecies.org:taxname:1",
                    false,
                    "checklistkey1"),
                new EqualsPredicate<>(
                    OccurrenceSearchParameter.TAXON_KEY,
                    "urn:lsid:marinespecies.org:taxname:2",
                    false,
                    "checklistkey2")));
    try {
      String query = visitor.buildQuery(predicate);
      String expectedQuery =
          "{\n"
              + "  \"bool\" : {\n"
              + "    \"filter\" : [\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"filter\" : [\n"
              + "            {\n"
              + "              \"term\" : {\n"
              + "                \"classifications.checklistkey1.taxonKeys\" : {\n"
              + "                  \"value\" : \"urn:lsid:marinespecies.org:taxname:1\",\n"
              + "                  \"boost\" : 1.0\n"
              + "                }\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      },\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"filter\" : [\n"
              + "            {\n"
              + "              \"term\" : {\n"
              + "                \"classifications.checklistkey2.taxonKeys\" : {\n"
              + "                  \"value\" : \"urn:lsid:marinespecies.org:taxname:2\",\n"
              + "                  \"boost\" : 1.0\n"
              + "                }\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      }\n"
              + "    ],\n"
              + "    \"adjust_pure_negative\" : true,\n"
              + "    \"boost\" : 1.0\n"
              + "  }\n"
              + "}";
      System.out.println(query);
      assertEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  /**
   * Tests queries with 2 taxon keys coming from 2 separate classification with disjunction
   * predicate (OR).
   */
  @Test
  public void testMultipleTaxonomiesDisjunctionPredicate() {

    DisjunctionPredicate predicate =
        new DisjunctionPredicate(
            Arrays.asList(
                new EqualsPredicate<>(
                    OccurrenceSearchParameter.TAXON_KEY,
                    "urn:lsid:marinespecies.org:taxname:1",
                    false,
                    "checklistkey1"),
                new EqualsPredicate<>(
                    OccurrenceSearchParameter.TAXON_KEY,
                    "urn:lsid:marinespecies.org:taxname:2",
                    false,
                    "checklistkey2")));
    try {
      String query = visitor.buildQuery(predicate);
      String expectedQuery =
          "{\n"
              + "  \"bool\" : {\n"
              + "    \"should\" : [\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"filter\" : [\n"
              + "            {\n"
              + "              \"term\" : {\n"
              + "                \"classifications.checklistkey1.taxonKeys\" : {\n"
              + "                  \"value\" : \"urn:lsid:marinespecies.org:taxname:1\",\n"
              + "                  \"boost\" : 1.0\n"
              + "                }\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      },\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"filter\" : [\n"
              + "            {\n"
              + "              \"term\" : {\n"
              + "                \"classifications.checklistkey2.taxonKeys\" : {\n"
              + "                  \"value\" : \"urn:lsid:marinespecies.org:taxname:2\",\n"
              + "                  \"boost\" : 1.0\n"
              + "                }\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      }\n"
              + "    ],\n"
              + "    \"adjust_pure_negative\" : true,\n"
              + "    \"boost\" : 1.0\n"
              + "  }\n"
              + "}";
      System.out.println(query);
      assertEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testIsNotNullTaxonKeyPredicate() {

    DisjunctionPredicate predicate =
        new DisjunctionPredicate(
            Arrays.asList(
                new IsNotNullPredicate<>(
                    OccurrenceSearchParameter.SPECIES_KEY, "test-checklist-key"),
                new IsNullPredicate<>(
                    OccurrenceSearchParameter.KINGDOM_KEY, "test-checklist-key")));
    try {
      String query = visitor.buildQuery(predicate);
      String expectedQuery =
          "{\n"
              + "  \"bool\" : {\n"
              + "    \"should\" : [\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"filter\" : [\n"
              + "            {\n"
              + "              \"exists\" : {\n"
              + "                \"field\" : \"classifications.test-checklist-key.taxonKeys\",\n"
              + "                \"boost\" : 1.0\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      },\n"
              + "      {\n"
              + "        \"bool\" : {\n"
              + "          \"filter\" : [\n"
              + "            {\n"
              + "              \"bool\" : {\n"
              + "                \"must_not\" : [\n"
              + "                  {\n"
              + "                    \"exists\" : {\n"
              + "                      \"field\" : \"classifications.test-checklist-key.taxonKeys\",\n"
              + "                      \"boost\" : 1.0\n"
              + "                    }\n"
              + "                  }\n"
              + "                ],\n"
              + "                \"adjust_pure_negative\" : true,\n"
              + "                \"boost\" : 1.0\n"
              + "              }\n"
              + "            }\n"
              + "          ],\n"
              + "          \"adjust_pure_negative\" : true,\n"
              + "          \"boost\" : 1.0\n"
              + "        }\n"
              + "      }\n"
              + "    ],\n"
              + "    \"adjust_pure_negative\" : true,\n"
              + "    \"boost\" : 1.0\n"
              + "  }\n"
              + "}";
      System.out.println(query);
      assertEquals(expectedQuery, query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }
}
