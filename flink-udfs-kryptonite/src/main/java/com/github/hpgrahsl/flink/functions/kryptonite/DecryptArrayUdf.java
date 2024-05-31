package com.github.hpgrahsl.flink.functions.kryptonite;

import java.lang.reflect.Array;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

public class DecryptArrayUdf extends AbstractCipherFieldUdf {

    @SuppressWarnings("unchecked")
    public @Nullable <T> T[] eval(@Nullable final String[] data, final T typeCapture) {
        if (data == null) {
            return null;
        }
        var result = (T[]) Array.newInstance(typeCapture.getClass(), data.length);
        for (int s = 0; s < data.length; s++) {
            result[s] = (T) decryptData(data[s]);
        }
        return result;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.ARRAY(DataTypes.STRING())),
                        InputTypeStrategies.ANY))
                .outputTypeStrategy(ctx -> {
                    var targetElementType = ctx.getArgumentDataTypes().get(1);
                    return Optional.of(DataTypes.ARRAY(targetElementType.nullable()));
                }).build();
    }

}
