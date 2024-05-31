package com.github.hpgrahsl.flink.functions.kryptonite;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

import com.github.hpgrahsl.kryptonite.FieldMetaData;
import com.github.hpgrahsl.kryptonite.crypto.tink.TinkAesGcm;

public class EncryptMapUdf extends AbstractCipherFieldUdf {

    private final static String CIPHER_ALGORITHM_DEFAULT = TinkAesGcm.CIPHER_ALGORITHM;

    private transient String defaultCipherDataKeyIdentifier;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        // TODO: load default key id via the function context
        defaultCipherDataKeyIdentifier = "keyA";
    }

    public @Nullable Map<?, String> eval(@Nullable final Object data) {
        if (data == null || !(data instanceof Map)) {
            return null;
        }
        return ((Map<?, ?>) data).entrySet().stream().map(
                e -> new AbstractMap.SimpleEntry<>(
                        e.getKey(),
                        encryptData(
                                e.getValue(),
                                new FieldMetaData(
                                        CIPHER_ALGORITHM_DEFAULT,
                                        Optional.ofNullable(e.getValue()).map(o -> o.getClass().getName()).orElse(""),
                                        defaultCipherDataKeyIdentifier))))
                .collect(LinkedHashMap::new, (lhm, e) -> lhm.put(e.getKey(), e.getValue()), HashMap::putAll);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.ANY // SHOULD BE LIMITED to "any map" i.e. MAP<K,V>
                        ))
                .outputTypeStrategy(ctx -> {
                    var targetKeyType = ((KeyValueDataType) ctx.getArgumentDataTypes().get(0)).getKeyDataType();
                    return Optional.of(DataTypes.MAP(targetKeyType, DataTypes.STRING()));
                })
                .build();
    }

}
