/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron.flink.table.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.auron.flink.table.AuronFlinkTableTestBase;
import org.apache.auron.flink.table.planner.UnsupportedFlinkNodeRecorder;
import org.apache.auron.flink.table.runtime.UdfInvocationTraceFixture.Event;
import org.apache.auron.flink.table.runtime.UdfInvocationTraceFixture.Phase;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

/**
 * IT case for Flink Calc Operator on Auron.
 */
public class AuronFlinkCalcITCase extends AuronFlinkTableTestBase {

    @Test
    public void testStageZeroSupportedPlusRunsNatively() throws Exception {
        UnsupportedFlinkNodeRecorder.resetForTest();
        TableResult result = tableEnvironment.executeSql("select `int` + 1 from T1");
        List<Row> rows;
        try (CloseableIterator<Row> iterator = result.collect()) {
            rows = CollectionUtil.iteratorToList(iterator);
        }
        assertThat(rows).containsExactlyInAnyOrder(Row.of(2), Row.of(3), Row.of(3));
        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount()).isZero();
    }

    @Test
    public void testScalarUdfRunsNativelyWithRealLifecycleTrace() throws Exception {
        UUID traceId = UdfInvocationTraceFixture.newTrace();
        try {
            environment.setParallelism(1);
            tableEnvironment.createTemporarySystemFunction("trace_add", new UdfInvocationTraceFixture(traceId, 7));
            UnsupportedFlinkNodeRecorder.resetForTest();
            TableResult result = tableEnvironment.executeSql("select trace_add(`int`), `int` + 1 from T1");
            List<Row> rows;
            try (CloseableIterator<Row> iterator = result.collect()) {
                rows = CollectionUtil.iteratorToList(iterator);
                result.getJobClient()
                        .orElseThrow(() -> new IllegalStateException("缺少本地作业客户端"))
                        .getJobExecutionResult()
                        .get();
            }
            assertThat(rows).containsExactlyInAnyOrder(Row.of(8, 2), Row.of(9, 3), Row.of(9, 3));
            assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount()).isZero();

            List<Event> events = UdfInvocationTraceFixture.snapshot(traceId);
            assertThat(events).as("本地真实执行必须共享静态 trace，不能用手工生命周期代替").isNotEmpty();
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.OPEN))
                    .isNotEmpty()
                    .allSatisfy(event -> assertThat(event.getSharedTraceClass()).isTrue());
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getArgument)
                    .containsExactlyInAnyOrder(1, 2, 2);
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getResult)
                    .containsExactlyInAnyOrder(8, 9, 9);
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .allSatisfy(event -> {
                        assertThat(event.getArgumentType()).isEqualTo(Integer.class);
                        assertThat(event.getResult()).isEqualTo((Integer) event.getArgument() + 7);
                    });
            assertThat(events.stream()
                            .collect(Collectors.groupingBy(Event::getRunId))
                            .values())
                    .allSatisfy(run -> {
                        assertThat(run.get(0).getPhase()).isEqualTo(Phase.OPEN);
                        assertThat(run.get(run.size() - 1).getPhase()).isEqualTo(Phase.CLOSE);
                        assertThat(run.subList(1, run.size() - 1)).allSatisfy(event -> assertThat(event.getPhase())
                                .isEqualTo(Phase.EVAL));
                        assertThat(run.stream()
                                        .filter(event -> event.getPhase() == Phase.EVAL)
                                        .map(Event::getThreadId)
                                        .distinct()
                                        .count())
                                .isLessThanOrEqualTo(1);
                        assertThat(run).allSatisfy(event -> assertThat(event.getThreadId())
                                .isPositive());
                        for (int i = 0; i < run.size(); i++) {
                            assertThat(run.get(i).getInvocationSequence()).isEqualTo(i + 1L);
                        }
                    });
        } finally {
            UdfInvocationTraceFixture.removeTrace(traceId);
        }
    }

    /** A UDF wrapped by an unsupported outer expression falls back to Flink codegen. The fallback
     * must preserve both the UDF lifecycle and the unsupported expression's result. */
    @Test
    public void testScalarUdfFallsBackWhenOuterExpressionIsUnsupported() throws Exception {
        UUID traceId = UdfInvocationTraceFixture.newTrace();
        try {
            environment.setParallelism(1);
            tableEnvironment.createTemporarySystemFunction("trace_add", new UdfInvocationTraceFixture(traceId, 7));
            UnsupportedFlinkNodeRecorder.resetForTest();

            List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                    .executeSql("select UPPER(trace_add(`string`)) from T1")
                    .collect());

            assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                    .as("an unsupported outer expression must take the whole Calc to Flink fallback")
                    .isPositive();
            rows.sort(Comparator.comparing(o -> (String) o.getField(0)));
            assertThat(rows).isEqualTo(Arrays.asList(Row.of("COMMENT#1:7"), Row.of("COMMENT#1:7"), Row.of("HI:7")));

            List<Event> events = UdfInvocationTraceFixture.snapshot(traceId);
            assertThat(events).isNotEmpty();
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.OPEN))
                    .hasSize(1);
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getArgumentType)
                    .containsOnly(String.class);
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getArgument)
                    .containsExactlyInAnyOrder("Hi", "Comment#1", "Comment#1");
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getResult)
                    .containsExactlyInAnyOrder("Hi:7", "Comment#1:7", "Comment#1:7");
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.CLOSE))
                    .hasSize(1);
        } finally {
            UdfInvocationTraceFixture.removeTrace(traceId);
        }
    }

    /** A convertible UDF in the same projection as an unsupported expression must not create a
     * half-native plan. The whole Calc falls back, and both columns keep Flink semantics. */
    @Test
    public void testScalarUdfWithUnsupportedExpressionInSameProjectionFallsBack() throws Exception {
        UUID traceId = UdfInvocationTraceFixture.newTrace();
        try {
            environment.setParallelism(1);
            tableEnvironment.createTemporarySystemFunction("trace_add", new UdfInvocationTraceFixture(traceId, 7));
            UnsupportedFlinkNodeRecorder.resetForTest();

            List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                    .executeSql("select trace_add(`string`), CHAR_LENGTH(`string`) from T1")
                    .collect());

            assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                    .as("an unsupported sibling expression must take the whole Calc to Flink fallback")
                    .isPositive();
            rows.sort(Comparator.comparing(o -> (String) o.getField(0)));
            assertThat(rows)
                    .isEqualTo(Arrays.asList(Row.of("Comment#1:7", 9), Row.of("Comment#1:7", 9), Row.of("Hi:7", 2)));

            List<Event> events = UdfInvocationTraceFixture.snapshot(traceId);
            assertThat(events).isNotEmpty();
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.OPEN))
                    .hasSize(1);
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getArgument)
                    .containsExactlyInAnyOrder("Hi", "Comment#1", "Comment#1");
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getResult)
                    .containsExactlyInAnyOrder("Hi:7", "Comment#1:7", "Comment#1:7");
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.CLOSE))
                    .hasSize(1);
        } finally {
            UdfInvocationTraceFixture.removeTrace(traceId);
        }
    }

    /** A STRING UDF over a nullable column runs natively and preserves NULL propagation end to
     * end. This complements the Arrow-FFI unit tests with the SQL planner and MiniCluster path. */
    @Test
    public void testNullableStringUdfRunsNativelyWithNullPropagation() throws Exception {
        UUID traceId = UdfInvocationTraceFixture.newTrace();
        try {
            environment.setParallelism(1);
            String dataId =
                    TestValuesTableFactory.registerData(Arrays.asList(row("Hi"), row((String) null), row("Comment#1")));
            tableEnvironment.executeSql(" CREATE TABLE T_STRING_NULL ( "
                    + "`string` STRING "
                    + ") WITH ( "
                    + "'connector' = 'values',"
                    + "'data-id' = '" + dataId + "',"
                    + "'failing-source' = 'false' "
                    + ")");
            tableEnvironment.createTemporarySystemFunction("trace_add", new UdfInvocationTraceFixture(traceId, 7));
            UnsupportedFlinkNodeRecorder.resetForTest();

            List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                    .executeSql("select trace_add(`string`) from T_STRING_NULL")
                    .collect());

            assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                    .as("a primitive nullable STRING UDF call must run natively")
                    .isZero();
            assertThat(rows).containsExactlyInAnyOrder(Row.of("Hi:7"), Row.of((Object) null), Row.of("Comment#1:7"));

            List<Event> events = UdfInvocationTraceFixture.snapshot(traceId);
            assertThat(events).isNotEmpty();
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.OPEN))
                    .hasSize(1);
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getArgumentType)
                    .containsOnly(String.class);
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getArgument)
                    .containsExactlyInAnyOrder("Hi", null, "Comment#1");
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                    .extracting(Event::getResult)
                    .containsExactlyInAnyOrder("Hi:7", null, "Comment#1:7");
            assertThat(events.stream().filter(event -> event.getPhase() == Phase.CLOSE))
                    .hasSize(1);
        } finally {
            UdfInvocationTraceFixture.removeTrace(traceId);
        }
    }

    @Test
    public void testPlus() {
        List<Row> rows = CollectionUtil.iteratorToList(
                tableEnvironment.executeSql("select `int` + 1 from T1").collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2), Row.of(3), Row.of(3)));
    }

    /** An equality filter keeps only rows whose value matches. */
    @Test
    public void testFilterEquals() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` = 1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1)));
    }

    /** A not-equals filter drops rows whose value matches. */
    @Test
    public void testFilterNotEquals() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` <> 1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2), Row.of(2)));
    }

    /** A greater-than filter keeps only rows strictly above the bound. */
    @Test
    public void testFilterGreaterThan() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` > 1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2), Row.of(2)));
    }

    /** A less-than filter keeps only rows strictly below the bound. */
    @Test
    public void testFilterLessThan() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` < 2")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1)));
    }

    /** A greater-or-equal filter keeps rows at or above the bound. */
    @Test
    public void testFilterGreaterEqual() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` >= 1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1), Row.of(2), Row.of(2)));
    }

    /** A less-or-equal filter keeps rows at or below the bound. */
    @Test
    public void testFilterLessEqual() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` <= 2")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1), Row.of(2), Row.of(2)));
    }

    /** A comparison used directly in the projection yields a Boolean column per row. */
    @Test
    public void testComparisonInBooleanProjection() {
        List<Row> rows = CollectionUtil.iteratorToList(
                tableEnvironment.executeSql("select `int` > 1 from T1").collect());
        rows.sort(Comparator.comparing(o -> (Boolean) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(false), Row.of(true), Row.of(true)));
    }

    /** A filter comparing an INT column against a DOUBLE column exercises INT-to-DOUBLE operand
     * promotion; no row has int greater than double, so the result is empty. */
    @Test
    public void testMixedTypePromotionFilter() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` > `double`")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Collections.emptyList());
    }

    /** A LIKE filter keeps rows whose string matches the pattern. */
    @Test
    public void testFilterLike() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `string` from T1 where `string` LIKE 'Comment%'")
                .collect());
        rows.sort(Comparator.comparing(o -> (String) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of("Comment#1"), Row.of("Comment#1")));
    }

    /**
     * A string that cannot be parsed as INT under TRY_CAST resolves to NULL via the native
     * try-cast path instead of failing the query, confirming the converter routes the
     * TRY_CAST operator to the null-on-failure native node end to end.
     */
    @Test
    public void testTryCastUnparseableStringToInt() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select try_cast(`string` as INT) from T1")
                .collect());
        // Every `string` value ("Hi", "Comment#1") is non-numeric → all NULL.
        assertThat(rows).isEqualTo(Arrays.asList(Row.of((Object) null), Row.of((Object) null), Row.of((Object) null)));
    }

    /** A valid numeric-to-numeric CAST converts to the strict native cast node and yields the
     * widened values. */
    @Test
    public void testCastIntToDouble() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select cast(`int` as DOUBLE) from T1")
                .collect());
        rows.sort(Comparator.comparingDouble(o -> (double) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1d), Row.of(2d), Row.of(2d)));
    }

    /** A string-to-numeric CAST over a parseable per-row string converts to the strict native cast
     * node and yields the parsed values. */
    @Test
    public void testCastStringToInt() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select cast(cast(`int` as STRING) as INT) from T1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1), Row.of(2), Row.of(2)));
    }

    /** A boolean-to-string CAST over a per-row comparison converts to the strict native cast node
     * and renders each boolean as its lowercase textual form. */
    @Test
    public void testCastBooleanToString() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select cast((`int` > 1) as STRING) from T1")
                .collect());
        rows.sort(Comparator.comparing(o -> (String) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of("false"), Row.of("true"), Row.of("true")));
    }

    /** A string-to-boolean CAST over a per-row boolean rendered as text round-trips back to the
     * original boolean values through the strict native cast node. */
    @Test
    public void testCastStringToBoolean() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select cast(cast((`int` > 1) as STRING) as BOOLEAN) from T1")
                .collect());
        rows.sort(Comparator.comparing(o -> (Boolean) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(false), Row.of(true), Row.of(true)));
    }

    /** A cast to an unsupported target type (TIMESTAMP) is gated to Flink fallback and
     * still produces the correct row set. */
    @Test
    public void testCastToUnsupportedTypeFallsBack() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where cast(`ts` as TIMESTAMP) is not null")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1), Row.of(2), Row.of(2)));
    }

    /** A compound filter ANDing two comparisons on different columns keeps only rows satisfying both;
     * each comparison excludes a different row. */
    @Test
    public void testFilterAndComparison() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` <> 1 AND `ts` = '2020-10-10 00:00:02'")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2)));
    }

    /** A compound filter ORing two comparisons on different columns keeps rows satisfying either;
     * both operands contribute a distinct row. */
    @Test
    public void testFilterOrComparison() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` = 1 OR `ts` = '2020-10-10 00:00:03'")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1), Row.of(2)));
    }

    /** UNIX_TIMESTAMP over the per-row {@code ts} string converts to the native ext function and
     * yields the epoch seconds. The session timezone is set to Asia/Shanghai to make the result
     * deterministic and to exercise timezone propagation into the native plan. */
    @Test
    public void testUnixTimestamp() {
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select UNIX_TIMESTAMP(`ts`) from T1")
                .collect());
        rows.sort(Comparator.comparingLong(o -> (long) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1602259201L), Row.of(1602259202L), Row.of(1602259203L)));
    }

    /** UNIX_TIMESTAMP yields the epoch seconds at zero offset when the session timezone is named
     * without a region prefix. */
    @Test
    public void testUnixTimestampUtcTimeZone() {
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select UNIX_TIMESTAMP(`ts`) from T1")
                .collect());
        rows.sort(Comparator.comparingLong(o -> (long) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1602288001L), Row.of(1602288002L), Row.of(1602288003L)));
    }

    /**
     * A fixed-offset session timezone names a constant offset rather than a region. The native
     * function parses the offset instead of looking it up, so the Calc converts and applies it.
     *
     * <p>Flink's codegen Calc produces the same rows from the same offset, so the values alone
     * cannot show which engine ran. The fallback counter is what discriminates: a Calc that fails
     * to convert records either an unsupported node or a composition failure before falling back,
     * so a count of zero means this Calc converted. The row set carries the rest — a plan that
     * converts but hits no native registry arm fails while executing, where nothing records a
     * fallback, and surfaces as an empty result with the counter still at zero.
     */
    @Test
    public void testUnixTimestampFixedOffsetTimeZoneRunsNatively() {
        UnsupportedFlinkNodeRecorder.resetForTest();
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("GMT-08:00"));
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select UNIX_TIMESTAMP(`ts`) from T1")
                .collect());

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparingLong(o -> (long) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1602316801L), Row.of(1602316802L), Row.of(1602316803L)));
    }

    /**
     * The zero-argument UNIX_TIMESTAMP reads the wall clock rather than parsing a column. It yields
     * one row per input row, each carrying epoch seconds bracketed by the test's own clock reads.
     *
     * <p>Fallback to Flink's codegen Calc is silent and total, and produces an identical row set, so
     * the values alone cannot show the Calc ran natively. The fallback counter narrows it: a Calc
     * that fails to convert always records either an unsupported node or a composition failure
     * before it falls back, so a count of zero means this Calc converted. It does not mean the Calc
     * ran. A native library holding no registry arm for the function still converts at plan time
     * and only fails once executing, where nothing records a fallback, leaving the counter at zero
     * and the result set empty.
     *
     * <p>The row count is what establishes that the native plan executed. {@code allSatisfy} passes
     * vacuously over an empty list, so dropping {@code hasSize(3)} would let that runtime failure
     * read as success and leave native execution unverified.
     */
    @Test
    public void testUnixTimestampZeroArgRunsNatively() {
        UnsupportedFlinkNodeRecorder.resetForTest();
        long before = System.currentTimeMillis() / 1000;
        List<Row> rows = CollectionUtil.iteratorToList(
                tableEnvironment.executeSql("select UNIX_TIMESTAMP() from T1").collect());
        long after = System.currentTimeMillis() / 1000;

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        assertThat(rows)
                .as("one clock-bracketed row per input row; an empty result set means the native"
                        + " library implements no arm for this function")
                .hasSize(3)
                .allSatisfy(row -> assertThat((long) row.getField(0)).isBetween(before, after));
    }

    /** A NOT LIKE filter keeps rows whose string does not match the pattern. */
    @Test
    public void testFilterNotLike() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `string` from T1 where `string` NOT LIKE 'Comment%'")
                .collect());
        rows.sort(Comparator.comparing(o -> (String) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of("Hi")));
    }
}
