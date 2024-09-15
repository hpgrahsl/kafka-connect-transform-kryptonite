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
import com.github.hpgrahsl.kryptonite.config.KryptoniteSettings;

public class EncryptMapUdf extends AbstractCipherFieldUdf {

    private transient String defaultCipherDataKeyIdentifier;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        defaultCipherDataKeyIdentifier = context.getJobParameter(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER,KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER_DEFAULT);
        if(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER_DEFAULT.equals(defaultCipherDataKeyIdentifier)) {
            defaultCipherDataKeyIdentifier = Optional.ofNullable(System.getenv(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER)).orElse(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER_DEFAULT);
        }
    }

    public @Nullable Map<?, String> eval(@Nullable final Object data) {
        return eval(data, defaultCipherDataKeyIdentifier, KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    public @Nullable Map<?, String> eval(@Nullable final Object data, String keyIdentifier) {
        return eval(data, keyIdentifier, KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    public @Nullable Map<?, String> eval(@Nullable final Object data, String keyIdentifier, String cipherAlgorithm) {
        if (data == null || !(data instanceof Map)) {
            return null;
        }
        return ((Map<?, ?>) data).entrySet().stream().map(
                e -> new AbstractMap.SimpleEntry<>(
                        e.getKey(),
                        encryptData(
                                e.getValue(),
                                new FieldMetaData(
                                        cipherAlgorithm,
                                        Optional.ofNullable(e.getValue()).map(o -> o.getClass().getName()).orElse(""),
                                        keyIdentifier))))
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
