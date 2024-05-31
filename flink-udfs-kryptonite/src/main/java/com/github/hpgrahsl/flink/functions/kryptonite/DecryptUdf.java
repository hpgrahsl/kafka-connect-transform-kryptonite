package com.github.hpgrahsl.flink.functions.kryptonite;

import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

public class DecryptUdf extends AbstractCipherFieldUdf {

    @SuppressWarnings("unchecked")
    public @Nullable <T> T eval(@Nullable final String data, final T typeCapture) {
        if(data == null) {
            return null;
        }
        return (T) decryptData(data);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.STRING()),
                        InputTypeStrategies.ANY))
                .outputTypeStrategy(ctx -> {
                    var targetType = ctx.getArgumentDataTypes().get(1);
                    return Optional.of(targetType);
                }).build();
    }

}
