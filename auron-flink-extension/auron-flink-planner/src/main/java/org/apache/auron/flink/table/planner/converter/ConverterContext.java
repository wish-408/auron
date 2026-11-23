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
package org.apache.auron.flink.table.planner.converter;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.types.logical.RowType;

/**
 * Provides shared state to {@link FlinkNodeConverter} implementations during conversion.
 *
 * <p>Carries the input schema, configuration, and classloader needed for type-aware conversion
 * of Flink expressions and aggregate calls, plus the per-Calc UDF conversion state: a conversion
 * scope identifying the original Calc, and the registration-order counter that stamps each
 * converted UDF call.
 *
 * <p>UDF conversion is enabled only by the constructor that receives a {@code udfScope}; the
 * scope names the original Calc whose functions must not merge with those of any other Calc.
 * Support probing ({@link FlinkRexNodeConverter#isSupported}) consumes no registration order and
 * never touches the UDF instance — order numbers are handed out only on the final conversion of
 * a call, so a probe or a failed conversion leaves no state behind.
 */
public class ConverterContext {

    private final ReadableConfig tableConfig;
    private final AuronConfiguration auronConfiguration;
    private final ClassLoader classLoader;
    private final RowType inputType;
    private final String udfScope;
    private long udfRegistrationOrder;

    /**
     * Creates a new converter context with UDF conversion disabled.
     *
     * @param tableConfig Flink table-level configuration
     * @param auronConfiguration Auron-specific configuration, may be {@code null}
     * @param classLoader classloader for the current Flink context
     * @param inputType input schema of the node being converted
     */
    public ConverterContext(
            ReadableConfig tableConfig,
            AuronConfiguration auronConfiguration,
            ClassLoader classLoader,
            RowType inputType) {
        this(tableConfig, auronConfiguration, classLoader, inputType, null);
    }

    /**
     * Creates a new converter context whose UDF conversion support is governed by
     * {@code udfScope}: a non-null scope enables converting user-defined ScalarFunction calls
     * (payloads carry the scope), while {@code null} leaves them unsupported.
     *
     * @param tableConfig Flink table-level configuration
     * @param auronConfiguration Auron-specific configuration, may be {@code null}
     * @param classLoader classloader for the current Flink context
     * @param inputType input schema of the node being converted
     * @param udfScope per-Calc UDF conversion scope, or {@code null} to disable UDF conversion
     */
    public ConverterContext(
            ReadableConfig tableConfig,
            AuronConfiguration auronConfiguration,
            ClassLoader classLoader,
            RowType inputType,
            @Nullable String udfScope) {
        this.tableConfig = Objects.requireNonNull(tableConfig, "tableConfig must not be null");
        this.auronConfiguration = auronConfiguration;
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader must not be null");
        this.inputType = Objects.requireNonNull(inputType, "inputType must not be null");
        if (udfScope != null && udfScope.isEmpty()) {
            throw new IllegalArgumentException("udfScope must not be empty when provided");
        }
        this.udfScope = udfScope;
    }

    /** Returns the Flink table-level configuration. */
    public ReadableConfig getTableConfig() {
        return tableConfig;
    }

    /** Returns the Auron-specific configuration, or {@code null} if not provided. */
    public AuronConfiguration getAuronConfiguration() {
        return auronConfiguration;
    }

    /** Returns the classloader for the current Flink context. */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Returns the input schema of the node being converted.
     *
     * <p>Converters use this to resolve {@code RexInputRef} column references to concrete types,
     * check type support, and determine if casts are needed.
     */
    public RowType getInputType() {
        return inputType;
    }

    /**
     * Returns whether user-defined ScalarFunction calls convert to native wrapper nodes in this
     * conversion pass. Callers that have not staged a UDF lifecycle owner (e.g. the source fusion
     * pass until its owner is wired) convert with UDF conversion disabled so such Calcs stay
     * unsupported and run through a path that owns the UDF lifecycle.
     */
    public boolean isUdfConversionEnabled() {
        return udfScope != null;
    }

    /**
     * Returns the per-Calc UDF conversion scope stamped into every payload of this pass.
     *
     * @throws IllegalStateException if UDF conversion is disabled in this context
     */
    public String getUdfScope() {
        if (udfScope == null) {
            throw new IllegalStateException("UDF conversion is disabled in this context");
        }
        return udfScope;
    }

    /**
     * Consumes and returns the next UDF registration order. Order numbers grow by one per
     * converted call in conversion (visit) order, mirroring the native Calc codegen's function
     * registration sequence: condition first, then projection columns, nested calls in operand
     * order.
     */
    public long nextUdfRegistrationOrder() {
        return udfRegistrationOrder++;
    }
}
