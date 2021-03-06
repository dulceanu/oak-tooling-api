/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.tooling.filestore;

import static java.util.Collections.emptyList;
import static java.util.Objects.hash;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An instance of this interface represents a property of a segment store.
 * Two instances of the same subtype of {@code Property} are equal (according
 * to {@link #equals(Object)}) iff they are structurally equal. That is iff
 * they have equal values. Two instances of {@code Property} obtained from
 * different instances of {@code Store} can't be compared and attempting to
 * do so will throw an {@link IllegalArgumentException}.
 * <p>
 * <em>Implementation note:</em> the {@link #EQ} predicate can be used to
 * determine structural equality of {@code Property} instances if those cannot
 * come up with a more efficient implementation.
 */
public interface Property {

    /**
     * The singleton instances of this class represent the type of a property
     * and its representation as a Java type. These types are in direct
     * correspondence to the (no multi valued) types of the JCR specification.
     * @param <T>  The Java type used to represent an instance of {@code Type<>}
     */
    final class Type<T> {
        /**
         * A map of all types from type name to the actual type.
         */
        @Nonnull
        public static final Map<String, Type<?>> ALL = new HashMap<>();

        /**
         * Type of the {@link #NULL_PROPERTY}. No other property has this type.
         */
        @Nonnull
        public static final Type<Void> VOID =
                new Type<>(Void.class, "VOID");

        /**
         * Type of string properties.
         */
        @Nonnull
        public static final Type<String> STRING =
                new Type<>(String.class, "STRING");

        /**
         * Type of binary properties.
         */
        @Nonnull
        public static final Type<Binary> BINARY =
                new Type<>(Binary.class, "BINARY");

        /**
         * Type of integer properties.
         */
        @Nonnull
        public static final Type<Long> LONG =
                new Type<>(Long.class, "LONG");

        /**
         * Type of double properties.
         */
        @Nonnull
        public static final Type<Double> DOUBLE =
                new Type<>(Double.class, "DOUBLE");

        /**
         * Type of date properties.
         */
        @Nonnull
        public static final Type<String> DATE =
                new Type<>(String.class, "DATE");

        /**
         * Type of boolean properties.
         */
        @Nonnull
        public static final Type<Boolean> BOOLEAN =
                new Type<>(Boolean.class, "BOOLEAN");

        /**
         * Type of name properties.
         */
        @Nonnull
        public static final Type<String> NAME =
                new Type<>(String.class, "NAME");

        /**
         * Type of path properties.
         */
        @Nonnull
        public static final Type<String> PATH =
                new Type<>(String.class, "PATH");

        /**
         * Type of reference properties.
         */
        @Nonnull
        public static final Type<String> REFERENCE =
                new Type<>(String.class, "REFERENCE");

        /**
         * Type of weak reference properties.
         */
        @Nonnull
        public static final Type<String> WEAKREFERENCE =
                new Type<>(String.class, "WEAKREFERENCE");

        /**
         * Type of URI properties.
         */
        @Nonnull
        public static final Type<String> URI =
                new Type<>(String.class, "URI");

        /**
         * Type of decimal properties.
         */
        @Nonnull
        public static final Type<BigDecimal> DECIMAL =
                new Type<>(BigDecimal.class, "DECIMAL");

        @Nonnull
        private final Class<T> type;

        @Nonnull
        private final String name;

        private Type(@Nonnull Class<T> type, @Nonnull String name) {
            this.type = type;
            this.name = name;
            ALL.put(name, this);
        }

        /**
         * @return  the Java type representing this type.
         */
        @Nonnull
        public Class<T> getType() {
            return type;
        }

        /**
         * @return  the name of this type.
         */
        @Nonnull
        public String getName() {
            return name;
        }

        /**
         * Two types are equal iff both have the same name and the same representation.
         */
        @Override
        public boolean equals(@Nullable Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Type<?> that = (Type<?>) other;
            return Objects.equals(type, that.type) &&
                    Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return hash(type, name);
        }

        @Override
        public String toString() {
            return "Property.Type{" +
                    "type=" + type +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    /**
     * Singleton instance to represent non existing properties.
     */
    Property NULL_PROPERTY = new Property() {

        /**
         * @return  the empty string.
         */
        @Nonnull
        @Override
        public String getName() {
            return "";
        }

        /**
         * @return  {@link Type#VOID}
         */
        @Nonnull
        @Override
        public Type<?> type() {
            return Type.VOID;
        }

        /**
         * @return  0
         */
        @Override
        public int cardinality() {
            return 0;
        }

        /**
         * @throws IndexOutOfBoundsException  always
         */
        @Nonnull
        @Override
        public <T> T value(Type<T> type, int index) {
            throw new IndexOutOfBoundsException("No element at index " + index);
        }

        /**
         * @return  always empty.
         */
        @Nonnull
        @Override
        public <T> Iterable<T> values(Type<T> type) {
            return emptyList();
        }

        /**
         * @param other
         * @return  {@code true} iff {@code other == this == NULL_PROPERTY}
         */
        @Override
        public boolean equals(@Nonnull Object other) {
            return other instanceof Property && EQ.test(this, (Property) other);
        }

        /**
         * @return {@code "NULL_NODE"}
         */
        @Override
        public String toString() {
            return "NULL_PROPERTY";
        }

        /**
         * @return 0
         */
        @Override
        public int hashCode() {
            return 0;
        }
    };

    /**
     * Predicate representing structural equality of {@code Node}
     * instances. Two {@code Property} instances are structural equal iff
     * they have equal values, except for the {@link #NULL_PROPERTY}, which
     * is only equal to itself.
     */
    BiPredicate<Property, Property> EQ = (property1, property2) -> {
        if (property1 == property2) {
            return true;
        }
        if (property1 == NULL_PROPERTY || property2 == NULL_PROPERTY) {
            return false;
        }

        if (property1.type() != property2.type()) {
            return false;
        }

        if (property1.cardinality() != property2.cardinality()) {
            return false;
        }

        Type<?> type = property1.type();
        for (int k = 0; k < property1.cardinality(); k++) {
            if (type == Type.BINARY) {
                Binary v1 = property1.value(Type.BINARY, k);
                Binary v2 = property2.value(Type.BINARY, k);
                if (!Binary.EQ.test(v1, v2)) {
                    return false;
                }
            } else {
                Object v1 = property1.value(type, k);
                Object v2 = property2.value(type, k);
                if (!v1.equals(v2)) {
                    return false;
                }
            }
        }

        return true;
    };

    /**
     * @return  the name of this property.
     */
    @Nonnull
    String getName();

    /**
     * @return  the type of this property.
     */
    @Nonnull
    Type<?> type();

    /**
     * The cardinality of a property returns the number of values
     * a property holds.
     * @return  the cardinality of this property.
     */
    int cardinality();

    /**
     * Retrieve the value of this property at the given {@code index}. If
     * the specified {@code type} does not match this properties
     * {@link #type() type} a type conversion is performed if supported by
     * the underlying implementation.
     * @param type    target type for the value
     * @param index   index of the value
     * @param <T>     target type for the value
     * @return  the value at {@code index} having type {@code T}.
     * @throws UnsupportedOperationException if the requested conversion is not
     * supported by the underlying implementation.
     */
    @Nonnull
    <T> T value(Type<T> type, int index);

    /**
     * List of values in this property. The order is not specified. If
     * the specified {@code type} does not match this properties
     * {@link #type() type} a type conversion is performed if supported by
     * the underlying implementation.
     * @param type    target type for the value
     * @param <T>     target type for the value
     * @return  the values having type {@code T}.
     * @throws UnsupportedOperationException if the requested conversion is not
     * supported by the underlying implementation.
     */
    @Nonnull
    <T> Iterable<T> values(Type<T> type);
}
