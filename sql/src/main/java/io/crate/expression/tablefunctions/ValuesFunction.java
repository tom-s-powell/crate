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

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static io.crate.metadata.functions.params.Param.ANY_ARRAY;
import static io.crate.types.DataTypes.isArray;

public class ValuesFunction {

    public static final String NAME = "_values";
    private static final RelationName TABLE_IDENT = new RelationName("", NAME);

    private static class ValuesTableFunctionImplementation extends TableFunctionImplementation<List<Object>> {

        private final FunctionInfo info;

        private ValuesTableFunctionImplementation(FunctionInfo info) {
            this.info = info;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public final Iterable<Row> evaluate(TransactionContext txnCtx,
                                            Input<List<Object>>[] arguments) {
            return new ColumnOrientedRowsIterator(() -> iteratorsFrom(arguments));
        }

        private Iterator<Object>[] iteratorsFrom(Input<List<Object>>[] arguments) {
            Iterator[] iterators = new Iterator[arguments.length];
            for (int i = 0; i < arguments.length; i++) {
                var argument = arguments[i].value();
                if (argument == null) {
                    iterators[i] = Collections.emptyIterator();
                } else {
                    iterators[i] = argument.iterator();
                }
            }
            //noinspection unchecked
            return iterators;
        }

        @Override
        public ObjectType returnType() {
            // TODO:
            return (ObjectType) info.returnType();
        }
    }

    public static void register(TableFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(
            FuncParams.builder().withIndependentVarArgs(ANY_ARRAY).build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                DataType returnType;
                if (dataTypes.size() == 1) {
                    var dataType = dataTypes.get(0);
                    if (isArray(dataType)) {
                        returnType = ((ArrayType) dataType).innerType();
                    } else {
                        throw new IllegalArgumentException(
                            "Function argument must have an array data type, but was '" + dataType + "'");
                    }
                } else {
                    returnType = ObjectType.untyped();
                }
                return new ValuesTableFunctionImplementation(
                    new FunctionInfo(new FunctionIdent(NAME, dataTypes), returnType, FunctionInfo.Type.TABLE));
            }
        });
    }
}
