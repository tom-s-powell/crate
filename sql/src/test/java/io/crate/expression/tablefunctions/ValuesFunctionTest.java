/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.tablefunctions;

import io.crate.metadata.FunctionIdent;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class ValuesFunctionTest extends AbstractTableFunctionsTest {

    @Test
    public void test_one_col_one_row() {
        assertExecute("_values([1])", "1\n");
    }

    @Test
    public void test_two_mixed_cols() {
        assertExecute("_values([1, 2], ['a', 'b'])",
                      "1| a\n" +
                      "2| b\n");
    }

    @Test
    public void test_first_array_shorter() {
        assertExecute("_values([1, 2], [1, 2, 3])",
                      "1| 1\n" +
                      "2| 2\n" +
                      "NULL| 3\n");
    }

    @Test
    public void test_second_array_shorter() {
        assertExecute("_values([1, 2, 3], [1, 2])",
                      "1| 1\n" +
                      "2| 2\n" +
                      "3| NULL\n");
    }

    @Test
    public void test_single_null_argument_returns_zero_rows() {
        assertExecute("_values(null)", "");
    }

    @Test
    public void test_null_and_array_with_null() {
        assertExecute("_values(null, [1, null])",
                      "NULL| 1\n" +
                      "NULL| NULL\n");
    }

    @Test
    public void test_does_not_flatten_nested_arrays() {
        assertExecute("_values([[1, 2], [3, 4, 5]])",
                      "[1, 2]\n" +
                      "[3, 4, 5]\n"
        );
    }

    @Test
    public void test_does_not_flatten_nested_arrays_with_mixed_depth() {
        assertExecute("_values([[1, 2], [3, 4, 5]], ['a', 'b'])",
                      "[1, 2]| a\n" +
                      "[3, 4, 5]| b\n"
        );
    }

    @Test
    public void test_function_return_type_of_the_next_nested_item() {
        List<DataType> argumentTypes = List.of(new ArrayType<>(new ArrayType<>(DataTypes.STRING)));
        var funcImplementation = functions.getQualified(
            new FunctionIdent(ValuesFunction.NAME, argumentTypes));

        assertThat(funcImplementation.info().returnType(), is(new ArrayType<>(DataTypes.STRING)));
    }

    @Test
    public void test_function_arguments_must_have_array_types() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Function argument must have an array data type, but was 'text'");
        functions.getQualified(new FunctionIdent(ValuesFunction.NAME, List.of(DataTypes.STRING)));
    }
}
