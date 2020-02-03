/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.language;

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoPointType;
import io.crate.types.GeoShapeType;
import io.crate.types.ObjectType;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.TypeLiteral;
import org.graalvm.polyglot.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.crate.operation.language.JavaScriptLanguage.resolvePolyglotFunctionValue;

public class JavaScriptUserDefinedFunction extends Scalar<Object, Object> {

    private final FunctionInfo info;
    private final String script;

    JavaScriptUserDefinedFunction(FunctionInfo info, String script) {
        this.info = info;
        this.script = script;
    }

    @Override
    public Scalar<Object, Object> compile(List<Symbol> arguments) {
        try {
            return new CompiledFunction(
                resolvePolyglotFunctionValue(
                    info.ident().name(),
                    script));
        } catch (Throwable e) {
            // this should not happen if the script was validated upfront
            throw new io.crate.exceptions.ScriptException(
                "compile error",
                e,
                JavaScriptLanguage.NAME
            );
        }
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, Input<Object>[] args) {
        try {
            var function = resolvePolyglotFunctionValue(info.ident().name(), script);
            return toCrateTypeValue(
                function.execute(materializeArgs(args)),
                info.returnType());
        } catch (PolyglotException | IOException e) {
            throw new io.crate.exceptions.ScriptException(
                e.getLocalizedMessage(),
                e,
                JavaScriptLanguage.NAME
            );
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    private class CompiledFunction extends Scalar<Object, Object> {

        private final Value function;

        private CompiledFunction(Value function) {
            this.function = function;
        }

        @Override
        public final Object evaluate(TransactionContext txnCtx, Input<Object>[] args) {
            try {
                return toCrateTypeValue(
                    function.execute(materializeArgs(args)),
                    info.returnType());
            } catch (PolyglotException e) {
                throw new io.crate.exceptions.ScriptException(
                    e.getLocalizedMessage(),
                    e,
                    JavaScriptLanguage.NAME
                );
            }
        }

        @Override
        public FunctionInfo info() {
            // Return the functionInfo of the outer class, because the function
            // info is the same for every compiled function instance.
            return info;
        }
    }

    private static Object[] materializeArgs(Input<Object>[] inputs) {
        Object[] args = new Object[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            args[i] = Value.asValue(inputs[i].value());
        }
        return args;
    }

    private static Object toCrateTypeValue(Value value, DataType<?> type) {
        if (value == null || "undefined".equalsIgnoreCase(value.getClass().getSimpleName())) {
            return null;
        }
        switch (type.id()) {
            case ArrayType.ID:
                ArrayList<Object> items = new ArrayList<>();
                for (int idx = 0; idx < value.getArraySize(); idx++) {
                    var item = toCrateTypeValue(value.getArrayElement(idx), ((ArrayType) type).innerType());
                    items.add(idx, item);
                }
                return type.value(items);
            case ObjectType.ID:
                return type.value(value.as(new TypeLiteral<Map>() {}));
            case GeoPointType.ID:
                if (value.hasArrayElements()) {
                    return type.value(toCrateTypeValue(value, DataTypes.DOUBLE_ARRAY));
                } else {
                    return type.value(value.asString());
                }
            case GeoShapeType.ID:
                if (value.isString()) {
                    return type.value(value.asString());
                } else {
                    return type.value(value.as(new TypeLiteral<Map>() {}));
                }
            default:
                final Object polyglotValue;
                if (value.isNumber()) {
                    polyglotValue = value.as(new TypeLiteral<Number>() {});
                } else if (value.isString()) {
                    polyglotValue = value.asString();
                } else if (value.isBoolean()) {
                    polyglotValue = value.asBoolean();
                } else {
                    polyglotValue = value.asString();
                }
                return type.value(polyglotValue);
        }
    }
}
